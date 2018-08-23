/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.deduplication

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.blockingschemes._
import de.hpi.ingestion.deduplication.models.Block
import de.hpi.ingestion.deduplication.models.config.AttributeConfig
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class DeduplicationCandidateExport extends SparkJob {
    sparkOptions("spark.yarn.executor.memoryOverhead") = "8192"

    appName = "Deduplication Candidate Export"
    configFile = "deduplication_annotation.xml"
    val blockingSchemes = List[BlockingScheme](
        SimpleBlockingScheme("simple_scheme"),
        LastLettersBlockingScheme("lastLetter_scheme"),
        MappedListBlockingScheme("mappedSectors_scheme", x => x.slice(0, 4), "gen_sectors"),
        MappedListBlockingScheme("mappedPostal_scheme", x => x.slice(0, 3), "geo_postal")
    )

    var subjects: RDD[Subject] = _
    var stagedSubjects: RDD[Subject] = _
    var duplicatesJson: RDD[String] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Subjects and the staged Subjects from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
        stagedSubjects = sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable"))
    }

    /**
      * Saves the duplicates to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        duplicatesJson.saveAsTextFile(s"deduplication_training_data_${System.currentTimeMillis()}")
    }
    // $COVERAGE-ON$

    /**
      * Blocks the Subjects and finds duplicates between them between the Subjects and staged Subjects.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val blocking = new Blocking
        settings.get("maxBlockSize").foreach(blocking.setMaxBlockSize)
        settings.get("minBlockSize").foreach(blocking.setMinBlockSize)
        settings.get("filterUndefined").foreach(blocking.setFilterUndefined)
        settings.get("filterSmall").foreach(blocking.setFilterSmall)
        val comparingWithMasterSubjects = settings("subjectTable") == "subject"
        blocking.subjects = subjects.filter(_.isSlave || !comparingWithMasterSubjects)
        blocking.stagedSubjects = stagedSubjects
        blocking.setPartitioning(sc)
        blocking.subjectReductionFunction = (subject: Subject) => {
            subject.master_history = Nil
            subject.name_history = Nil
            subject.category_history = Nil
            subject.properties_history = Map()
            subject.relations_history = Map()
            subject.aliases_history = Nil
            subject
        }

        // TODO: set blocking schemes
        val blocks = blocking.blocking().values
        val unnormalizedScoreConfig = scoreConfigSettings
            .map { aConf =>
                val scoreConf = aConf.scoreConfigs.map(_.copy[String, SimilarityMeasure[String]](weight = 1.0))
                aConf.copy(scoreConfigs = scoreConf, weight = 1.0)
            }
        val scoreConfigBroadcast = sc.broadcast(unnormalizedScoreConfig)
        val subjectPairs = findDuplicates(blocks, settings("confidence").toDouble, scoreConfigBroadcast)
        duplicatesJson = createDuplicates(subjectPairs, settings("stagingTable"), settings("subjectTable"))
    }

    /**
      * Compares two subjects according to the configuration and returns the scores for each similarity measure as well
      * as the mean of those scores.
      * @param subject1 the first Subject of the comparison
      * @param subject2 the second Subject of the comparison
      * @return tuple of the mean of the similarity scores and a list of the scores
      */
    def compare(
        subject1: Subject,
        subject2: Subject,
        attributeConfigs: List[AttributeConfig] = Nil,
        scale: Int = 1
    ): (Double, List[Double]) = {
        val scores = for {
            AttributeConfig(attribute, weight, configs) <- attributeConfigs
            subjectValues = subject1.get(attribute)
            stagingValues = subject2.get(attribute)
            if subjectValues.nonEmpty && stagingValues.nonEmpty
            config <- configs
        } yield CompareStrategy(attribute)(subjectValues, stagingValues, config) * weight
        (scores.sum / scores.length, scores)
    }

    /**
      * Groups all found duplicates by the staged Subjects. These groups are then serialized into JSON.
      * @param subjectPairs RDD containing all found duplicates and their scores
      * @param sourceTable source table of the existing Subjects
      * @param stagingTable source table of the staged Subjects
      * @return RDD serialized duplicate groups
      */
    def createDuplicates(
        subjectPairs: RDD[(Subject, Subject, List[Double], Double)],
        sourceTable: String,
        stagingTable: String
    ): RDD[String] = {
        subjectPairs
            .map { case (subject, staging, scores, mean) =>
                (staging.id, List((subject, staging, scores, mean)))
            }.reduceByKey(_ ::: _)
            .values
            .map { duplicateList =>
                val staging = duplicateList.head._2
                val subjects = duplicateList
                    .sortBy { case (subject, staging, scores, mean) => -mean }
                    .take(10)
                    .map { case (subject, staging, scores, mean) =>
                        Map(
                            "mean" -> mean,
                            "scores" -> scores,
                            "subject" -> subject
                        )
                    }
                val duplicateData = Map(
                    "staging" -> staging,
                    "staging_source_table" -> stagingTable,
                    "subject_source_table" -> sourceTable,
                    "subjects" -> subjects
                )
                JSONParser(duplicateData).toString
            }
    }

    /**
      * Finds the duplicates of each block by comparing the Subjects and filtering all Subjects pairs below the
      * threshold confidence. The comparison scores and their mean are added to all Subject pairs that are kept.
      * @param blocks RDD of Blocks that produced by the Blocking
      * @param confidence threshold used to filter duplicate candidates
      * @param scoreConfigBroadcast Broadcast of the score config
      * @return tuple of Subjects with the scores of the similarity measures and the mean of those
      */
    def findDuplicates(
        blocks: RDD[Block],
        confidence: Double,
        scoreConfigBroadcast: Broadcast[List[AttributeConfig]]
    ): RDD[(Subject, Subject, List[Double], Double)] = {
        blocks
            .flatMap(_.crossProduct())
            .map { case (subject1, subject2) =>
            val (mean, scores) = compare(subject1, subject2, scoreConfigBroadcast.value)
            (subject1, subject2, scores, mean)
        }.filter(_._4 >= confidence)
        .repartition(64)
    }
}
