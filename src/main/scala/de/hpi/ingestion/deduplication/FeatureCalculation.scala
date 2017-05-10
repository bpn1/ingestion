package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.blockingschemes.{BlockingScheme, SimpleBlockingScheme}
import de.hpi.ingestion.deduplication.models.{Block, FeatureEntry, ScoreConfig}
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.io.Source
import scala.xml.{Node, XML}

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
	var settings = Map[String, String]()
	var config = Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]()
	val configFile: Option[String] = None
	val blockingSchemes = List[BlockingScheme](SimpleBlockingScheme("simple_scheme"))

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

	def parseConfig(): Unit = {
		val path = this.configFile.getOrElse("config.xml")
		val xml = XML.loadString(Source
			.fromURL(getClass.getResource(s"/$path"))
			.getLines()
			.mkString("\n"))

		val configSettings = xml \\ "config" \ "sourceSettings"
		this.settings = configSettings.head.child
			.collect {
				case node: Node if node.text.trim.nonEmpty => (node.label, node.text)
			}.toMap

		val attributes = xml \\ "config" \ "simMeasurements" \ "attribute"
		this.config = attributes.map { node =>
			val key = (node \ "key").text
			val scoreConfigs = (node \ "feature").map { feature =>
				val similarityMeasure = (feature \ "similarityMeasure").text
				val weight = (feature \ "weight").text.toDouble
				val scale = (feature \ "scale").text.toInt
				ScoreConfig[String, SimilarityMeasure[String]](
					SimilarityMeasure.get[String](similarityMeasure),
					weight,
					scale)
			}
			(key, scoreConfigs.toList)
		}.toMap
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
		staging: Subject,
		config: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]
	): FeatureEntry = {
		val scores = config.map { case (attribute, scoreConfigs) =>
			val subjectValues = subject.get(attribute)
			val stagingValues = staging.get(attribute)
			val scores = scoreConfigs.map(compare(attribute, subjectValues, stagingValues, _))
			(attribute, scores)
		}
		FeatureEntry(subject, staging, scores)
	}

	/**
	  * Finds the duplicates of each block by comparing the Subjects and filtering all Subjects pairs below the
	  * threshold {@confidence}.
	  * @param blocks RDD of BLocks containing the Subjects that are compared
	  * @return tuple of Subjects with their score, which is greater or equal the given threshold.
	  */
	def findDuplicates(blocks: RDD[Block], sc: SparkContext): RDD[FeatureEntry] = {
		val threshold = this.settings("confidence").toDouble
		val confBroad = sc.broadcast(this.config)
		blocks
			.flatMap(_.crossProduct())
			.map { case (subject1, subject2) =>
				createFeature(subject1, subject2, confBroad.value)
			}.filter(_.scores.toList.flatMap(_._2).sum >= threshold)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val inputSubjects = input.fromAnyRDD[Subject]()
		val dbpedia = inputSubjects.head
		val wikidata = inputSubjects(1)
		val goldStandard = input.fromAnyRDD[(UUID, UUID)]().head

		this.parseConfig()
		val blocks = Blocking.blocking(dbpedia, wikidata, this.blockingSchemes)
		val features = findDuplicates(blocks.values, sc)
		val labeledPoints = labelFeature(features, goldStandard)
		List(labeledPoints).toAnyRDD()
	}
}
