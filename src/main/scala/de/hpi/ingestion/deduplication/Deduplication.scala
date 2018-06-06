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
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.deduplication.blockingschemes._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.models.config.AttributeConfig
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Compares two groups of Subjects by first blocking with multiple Blocking Schemes, then compares all Subjects
  * in every block and filters all pairs below a given threshold.
  */
class Deduplication extends SparkJob {
    import Deduplication._
    sparkOptions("spark.yarn.executor.memoryOverhead") = "8192"

    appName = "Deduplication"
    configFile = "deduplication.xml"
    val blockingSchemes = List[BlockingScheme](
        SimpleBlockingScheme("simple_scheme"),
        LastLettersBlockingScheme("lastLetter_scheme"),
        MappedListBlockingScheme("mappedSectors_scheme", x => x.slice(0, 4), "gen_sectors"),
        MappedListBlockingScheme("mappedPostal_scheme", x => x.slice(0, 3), "geo_postal")
    )

    var subjects: RDD[Subject] = _
    var stagedSubjects: RDD[Subject] = _
    var duplicates: RDD[Duplicates] = _
    var duplicates2: RDD[String] = _

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
//        duplicates.saveToCassandra(settings("keyspaceDuplicatesTable"), settings("duplicatesTable"))
        duplicates2.saveAsTextFile(s"deduplication_training_data_${System.currentTimeMillis()}")
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
        blocking.subjects = subjects//.filter(_.isSlave)
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
                aConf.copy(scoreConfigs = scoreConf)
            }
        val scoreConfigBroadcast = sc.broadcast(unnormalizedScoreConfig)
        val subjectPairs = findDuplicates2(blocks, settings("confidence").toDouble, scoreConfigBroadcast)
        duplicates2 = createDuplicates2(subjectPairs, settings("stagingTable"))
    }
}

object Deduplication {
    /**
      * Compares to subjects regarding the configuration.
      * @param subject1 subjects to be compared to subject2
      * @param subject2 subjects to be compared to subject1
      * @return the similarity score of the subjects
      */
    def compare(
        subject1: Subject,
        subject2: Subject,
        attributeConfigs: List[AttributeConfig] = Nil,
        scale: Int = 1
    ): Double = {
        val scores = for {
            AttributeConfig(attribute, weight, configs) <- attributeConfigs
            subjectValues = subject1.get(attribute)
            stagingValues = subject2.get(attribute)
            if subjectValues.nonEmpty && stagingValues.nonEmpty
            config <- configs
        } yield CompareStrategy(attribute)(subjectValues, stagingValues, config) * weight
        scores.sum / scores.length
    }

    def compare2(
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
      * Groups all found duplicates by the Subject whose duplicate they are and creates the corresponding
      * DuplicateCandidates.
      * @param subjectPairs RDD of Subject pairs containing all found duplicates
      * @param stagingTable source table of the staged Subjects
      * @return RDD of grouped Duplicate Candidates
      */
    def createDuplicates(
        subjectPairs: RDD[(Subject, Subject, Double)],
        stagingTable: String
    ): RDD[Duplicates] = {
        subjectPairs
            .map { case (subject, staging, score) =>
                ((subject.id, subject.name), List(Candidate(staging.id, staging.name, score)))
            }.reduceByKey(_ ::: _)
            .map { case ((id, name), candidates) => Duplicates(id, name, stagingTable, candidates.distinct) }
    }

    def createDuplicates2(
        subjectPairs: RDD[(Subject, Subject, List[Double], Double)],
        stagingTable: String
    ): RDD[String] = {
        subjectPairs
            .map { case (subject, staging, scores, mean) =>
                (staging.id, List((subject, staging, scores, mean)))
            }.reduceByKey(_ ::: _)
            .map { case (stagingId, subjectPairs) =>
                val staging = subjectPairs.head._2
                val subjects = subjectPairs.map { case (subject, staging, scores, mean) =>
                   Map(
                       "mean" -> JSONParser(mean),
                       "scores" -> JSONParser(scores),
                       "subject" -> JSONParser(subject)
//                       "mean" -> mean,
//                       "scores" -> scores,
//                       "subject" -> subject
                   )
                }
                val duplicateData = Map(
                    "staging" -> JSONParser(staging),
                    "staging_source_table" -> JSONParser(stagingTable),
                    "subject_source_table" -> JSONParser("implisense"),
                    "subjects" -> JSONParser(subjects)
//                    "staging" -> staging,
//                    "staging_source_table" -> stagingTable,
//                    "subject_source_table" -> "subject_implisense",
//                    "subjects" -> subjects
                )
                JSONParser(duplicateData).toString
            }
    }

    /**
      * Finds the duplicates of each block by comparing the Subjects and filtering all Subjects pairs below the
      * threshold confidence.
      * @param blocks RDD of BLocks containing the Subjects that are compared
      * @param confidence threshold used to filter duplicate candidates
      * @param scoreConfigBroadcast Broadcast of the used score config
      * @return tuple of Subjects with their score, which is greater or equal the given threshold.
      */
    def findDuplicates(
        blocks: RDD[Block],
        confidence: Double,
        scoreConfigBroadcast: Broadcast[List[AttributeConfig]]
    ): RDD[(Subject, Subject, Double)] = {
        blocks.flatMap { block =>
            val filterFunction = (s1: Subject, s2: Subject) =>
                compare(s1, s2, scoreConfigBroadcast.value) >= confidence
            block
                .crossProduct(filterFunction)
                .map { case (subject1, subject2) =>
                    val score = compare(subject1, subject2, scoreConfigBroadcast.value)
                    (subject1, subject2, score)
                }
        }
    }

    def findDuplicates2(
        blocks: RDD[Block],
        confidence: Double,
        scoreConfigBroadcast: Broadcast[List[AttributeConfig]]
    ): RDD[(Subject, Subject, List[Double], Double)] = {
        blocks.flatMap { block =>
            val filterFunction = (s1: Subject, s2: Subject) =>
                compare(s1, s2, scoreConfigBroadcast.value) >= confidence
            block
                .crossProduct(filterFunction)
                .map { case (subject1, subject2) =>
                    val (mean, scores) = compare2(subject1, subject2, scoreConfigBroadcast.value)
                    (subject1, subject2, scores, mean)
                }
        }
    }
}
