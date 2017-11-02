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

import java.util.UUID

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.blockingschemes.{BlockingScheme, SimpleBlockingScheme}
import de.hpi.ingestion.deduplication.models.config.{AttributeConfig, SimilarityMeasureConfig}
import de.hpi.ingestion.deduplication.models.{Block, FeatureEntry}
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure
import org.apache.spark.broadcast.Broadcast

/**
  * Job for calculation of feature entries
  */
class FeatureCalculation extends SparkJob {
    import FeatureCalculation._
    appName = "Feature calculation"
    configFile = "feature_calculation.xml"
    val blockingSchemes = List[BlockingScheme](SimpleBlockingScheme("simple_scheme"))

    var dbpediaSubjects: RDD[Subject] = _
    var wikidataSubjects: RDD[Subject] = _
    var goldStandard: RDD[(UUID, UUID)] = _
    var featureEntries: RDD[FeatureEntry] = _

    // $COVERAGE-OFF$
    /**
      * Loads the DBpedia and Wikidata Subjects and the gold standard from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        dbpediaSubjects = sc.cassandraTable[Subject](settings("keyspaceDBpediaTable"), settings("dBpediaTable"))
        wikidataSubjects = sc.cassandraTable[Subject](settings("keyspaceWikidataTable"), settings("wikiDataTable"))
        goldStandard = sc.cassandraTable[(UUID, UUID)](
            settings("keyspaceGoldStandardTable"),
            settings("goldStandardTable"))
    }

    /**
      * Saves the generated Feature Entries to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        featureEntries.saveToCassandra(settings("keyspaceFeatureTable"), settings("featureTable"))
    }
    // $COVERAGE-ON$

    /**
      * Generates Feature Entries using the DBpedia and Wikidata Subjects and the gold standard.
      * @param sc SparkContext to be used for the job
      */
    override def run(sc: SparkContext): Unit = {
        val blocking = new Blocking
        blocking.subjects = dbpediaSubjects
        blocking.stagedSubjects = wikidataSubjects
        blocking.blockingSchemes = blockingSchemes
        settings.get("maxBlockSize").foreach(blocking.setMaxBlockSize)
        val blocks = blocking.blocking()
        val scoreConfigBroadcast = sc.broadcast(scoreConfigSettings)
        val features = findDuplicates(blocks.values, scoreConfigBroadcast)
        featureEntries = labelFeature(features, goldStandard)
    }
}
object FeatureCalculation {
    /**
      * Labels the feature corresponding to a gold standard
      * @param features Features to be labeled
      * @param goldStandard The Goldstandard whom the labels are taken from
      * @return Features with correct labels
      */
    def labelFeature(features: RDD[FeatureEntry], goldStandard: RDD[(UUID, UUID)]): RDD[FeatureEntry] = {
        val keyedFeatures = features.keyBy(feature => (feature.subject.id, feature.staging.id))
        val keyedGoldStandard = goldStandard.map((_, true))

        keyedFeatures
            .leftOuterJoin(keyedGoldStandard)
            .values
            .map { case (feature, correct) =>
                feature.copy(correct = correct.getOrElse(false))
            }
    }

    /**
      * Compares to subjects regarding an attribute
      * @param attribute Attribute to be compared
      * @param subjectValues Values of the attribute of the subject
      * @param stagingValues Values of the attribute of the staging subject
      * @param scoreConfig configuration
      * @return Normalized score or 0.0 if one of the values are empty
      */
    def compare(
        attribute: String,
        subjectValues: List[String],
        stagingValues: List[String],
        scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]
    ): Double = (subjectValues, stagingValues) match {
        case (xs, ys) if xs.nonEmpty && ys.nonEmpty =>
            CompareStrategy(attribute)(subjectValues, stagingValues, scoreConfig)
        case _ => 0.0
    }

    /**
      * Creates a FeatureEntry from two given subjects and a config
      * @param subject subject
      * @param staging staging
      * @param attributeConfig configuration
      * @return FeatureEntry
      */
    def createFeature(
        subject: Subject,
        staging: Subject,
        attributeConfig: List[AttributeConfig]
    ): FeatureEntry = {
        val scores = attributeConfig.map { case AttributeConfig(attribute, weight, scoreConfigs) =>
            val subjectValues = subject.get(attribute)
            val stagingValues = staging.get(attribute)
            val scores = scoreConfigs.map(this.compare(attribute, subjectValues, stagingValues, _))
            attribute -> scores
        }.toMap
        FeatureEntry(subject = subject, staging = staging, scores = scores)
    }

    /**
      * Finds the duplicates of each block by comparing the Subjects
      * @param blocks RDD of BLocks containing the Subjects that are compared
      * @param scoreConfigBroadcast Broadcast of the used score config
      * @return tuple of Subjects with their score
      */
    def findDuplicates(
        blocks: RDD[Block],
        scoreConfigBroadcast: Broadcast[List[AttributeConfig]]
    ): RDD[FeatureEntry] = {
        blocks
            .flatMap(_.crossProduct())
            .map { case (subject1, subject2) => createFeature(subject1, subject2, scoreConfigBroadcast.value) }
    }
}
