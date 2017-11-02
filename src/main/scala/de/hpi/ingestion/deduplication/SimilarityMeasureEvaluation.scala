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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.models.{Duplicates, Candidate, PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.Bag

class SimilarityMeasureEvaluation extends SparkJob {
    import SimilarityMeasureEvaluation._
    appName = "SimilarityMeasureEvaluation"
    configFile = "similarity_measure_evaluation.xml"

    var duplicates: RDD[Duplicates] = _
    var goldStandard: RDD[(UUID, UUID)] = _
    var simMeasureStats: RDD[SimilarityMeasureStats] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Duplicates and the gold standard from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        duplicates = sc.cassandraTable[Duplicates](settings("keyspaceTrainingTable"), settings("trainingTable"))
        goldStandard = sc.cassandraTable[(UUID, UUID)](settings("keyspaceTestTable"), settings("testTable"))
    }

    /**
      * Saves the generated Similarity Measure statistics to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        simMeasureStats.saveToCassandra(settings("keyspaceSimMeasureStatsTable"), settings("simMeasureStatsTable"))
    }
    // $COVERAGE-ON$

    /**
      * Calculates precision, recall and f1 score using the training and test data.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val training = duplicates
            .flatMap { case Duplicates(subject_id, subject_name, datasource, candidates) =>
                candidates.map { case Candidate(candidate_id, candidate_name, score) =>
                    ((subject_id, candidate_id), score)
                }
            }.distinct
        val test = goldStandard.map(pair => (pair, 1.0))
        implicit val buckets = generateBuckets(this.settings("buckets").toInt)
        val predictionAndLabels = generatePredictionAndLabels(training, test)
        val data = generatePrecisionRecallData(predictionAndLabels)
        val stats = SimilarityMeasureStats(
            data = data,
            comment = Option("Naive Deduplication"),
            xaxis = Option("threshold"))
        simMeasureStats = sc.parallelize(List(stats))
    }
}

object SimilarityMeasureEvaluation {
    /**
      * Generates a List of Buckets regarding the Config
      * @return List of Doubles defining the buckets
      */
    def generateBuckets(size: Int): List[Double] = {
        (0 to size)
            .map(_.toDouble / size.toDouble)
            .toList
    }

    /**
      * Returns a bucket corresponding to a score
      * @param score The score
      * @param buckets The bucket List, to choose the correct bucket from
      * @return The bucket containing the score
      */
    def bucket(score: Double)(implicit buckets: List[Double]): Double = {
        val size = buckets.size - 1
        (score * size).toInt / size.toDouble
    }

    /**
      * Generates PrecisionRecallDataTuple from labeledPoints for each bucket
      * @param labeledPoints Labeled Points
      * @param buckets Buckets
      * @return List of PrecisionRecallDataTuple for each bucket
      */
    def generatePrecisionRecallData(labeledPoints: RDD[(Double, Double)])
        (implicit buckets: List[Double]): List[PrecisionRecallDataTuple] = {
        val bag = labeledPoints.map { case (prediction, label) =>
            val bucketLabel = bucket(prediction)
            val key = s"$label;$bucketLabel"
            Bag(key -> 1)
        }.reduce(_ ++ _).getCounts()

        buckets.map { bucket =>
            val positiveNegativeCount = bag
                .toList
                .map { case (key, value) =>
                    val Array(label, bucketLabel) = key.split(';')
                    val positiveOrNegative = if(bucketLabel.toDouble >= bucket) "p" else "n"
                    val trueOrFalse = if((label == "1.0") == (positiveOrNegative == "p")) "t" else "f"
                    (s"$trueOrFalse$positiveOrNegative", value)
                }.groupBy(_._1)
                .mapValues(_.map(_._2).sum)
                .map(identity)

            val tp = positiveNegativeCount.getOrElse("tp", 0)
            val fp = positiveNegativeCount.getOrElse("fp", 0)
            val fn = positiveNegativeCount.getOrElse("fn", 0)

            val precision = tp.toDouble / (tp + fp).toDouble
            val recall = tp.toDouble / (tp + fn).toDouble
            val fMeasure = (2 * recall * precision) / (recall + precision)
            PrecisionRecallDataTuple(
                bucket,
                if(precision.isNaN) 0.0 else precision,
                recall,
                if(fMeasure.isNaN) 0.0 else fMeasure
            )
        }
    }

    /**
      * Generate (prediction, label) tuples for evaluation
      * @param training Predictions from the training data
      * @param test Labels from the test data
      * @return RDD containing (prediction, label) tuples
      */
    def generatePredictionAndLabels(
        training: RDD[((UUID, UUID), Double)],
        test: RDD[((UUID, UUID), Double)]
    ): RDD[(Double, Double)] = {
        training
            .fullOuterJoin(test)
            .values
            .collect {
                case (prediction, label) if prediction.isDefined || label.isDefined =>
                    (prediction.getOrElse(0.0), label.getOrElse(0.0))
            }
    }
}
