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
import scala.collection.mutable

/**
  * Job for calculation of feature entries
  */
object FeatureCalculation extends SparkJob {
	appName = "Feature calculation"
	val keyspace = "evaluation"
	val dbpediaTable = "subject_dbpedia"
	val wikidataTable = "subject_wikidata"
	val goldStandardTable = "goldstandard"
	val featuresTable = "sim_measure_features"
	var config = mutable.Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]()

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.cassandraTable[Subject](keyspace, dbpediaTable)
		val wikidata = sc.cassandraTable[Subject](keyspace, wikidataTable)
		val goldStandard = sc.cassandraTable[(UUID, UUID)](keyspace, goldStandardTable)
		List(dbpedia, wikidata).toAnyRDD() ::: List(goldStandard).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[FeatureEntry]()
			.head
			.saveToCassandra(keyspace, featuresTable)
	}

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

	def compare(
		attribute: String,
		subjectValues: List[String],
		stagingValues: List[String],
		scoreConfig: ScoreConfig[String, SimilarityMeasure[String]]
	): Double = (subjectValues, stagingValues) match {
		case (xs, ys) if xs.nonEmpty && ys.nonEmpty =>
			CompareStrategy(attribute)(subjectValues, stagingValues, scoreConfig)
		case _ => 0.0
	}

	def createFeature(
		subject: Subject,
		staging: Subject
	): FeatureEntry = {
		val scores = this.config.map { case (attribute, scoreConfigs) =>
			val subjectValues = subject.get(attribute)
			val stagingValues = staging.get(attribute)
			val scores = scoreConfigs.map(compare(attribute, subjectValues, stagingValues, _))
			(attribute, scores)
		}.toMap
		FeatureEntry(subject, staging, scores)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val inputSubjects = input.fromAnyRDD[Subject]()
		val dbpedia = inputSubjects.head
		val wikidata = inputSubjects(1)
		val goldStandard = input.fromAnyRDD[(UUID, UUID)]().head

		//val blocks = Blocking.blocking()
		// blocks.foreach(block => createFeature)
		// val labeledPoints = labelFeature(blocks, goldStandard)
		// List(labeledPoints).toAnyRDD()
		Nil
	}
}
