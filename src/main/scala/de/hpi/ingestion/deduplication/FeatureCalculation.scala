package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.models.{FeatureEntry, ScoreConfig}
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Job for calculation of feature entries
  */
object FeatureCalculation extends SparkJob {
	appName = "Feature calculation"
	val keyspace = "evaluation"
	val dbpedia = "subject_dbpedia"
	val wikidata = "subject_wikidata"
	val goldStandard = "goldstandard"
	val features = "sim_measure_features"

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.cassandraTable[Subject](keyspace, dbpedia)
		val wikidata = sc.cassandraTable[Subject](keyspace, wikidata)
		val goldStandard = sc.cassandraTable[(UUID, UUID)](keyspace, goldStandard)
		List(dbpedia, wikidata).toAnyRDD() ::: List(goldStandard).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[FeatureEntry]()
			.head
			.saveToCassandra(keyspace, features)
	}

	def isDuplicate(subject: Subject, staging: Subject, goldStandard: RDD[(UUID, UUID)]): Boolean = false // TODO: nyi

	def compare(subject: Subject, staging: Subject): Double = 0.0 // TODO: nyi

	def createFeature(
		subject: Subject,
		staging: Subject,
		goldStandard: RDD[(UUID, UUID)],
		config: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]
	): FeatureEntry = {
		val scores = config.map { case (attribute, scoreConfigs) =>
			val subjectValues = subject.get(attribute)
			val stagingValues = staging.get(attribute)
			val scores = scoreConfigs.map { scoreConfig =>
				if(subjectValues.isEmpty || stagingValues.isEmpty){
					0.0
				}
				else {
					CompareStrategy(attribute)(subjectValues, stagingValues, scoreConfig)
				}
			}
			(attribute, scores)
		}
		val correct = isDuplicate(subject, staging, goldStandard)
		FeatureEntry(subject, staging, scores, correct)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val inputSubjects = input.fromAnyRDD[Subject]()
		val dbpedia = inputSubjects.head
		val wikidata = inputSubjects(1)
		val goldStandard = input.fromAnyRDD[(UUID, UUID)]().head

		Nil
	}
}
