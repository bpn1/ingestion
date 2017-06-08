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
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Job for calculation of feature entries
  */
object FeatureCalculation extends SparkJob {
	appName = "Feature calculation"
	configFile = "feature_calculation.xml"
	val blockingSchemes = List[BlockingScheme](SimpleBlockingScheme("simple_scheme"))

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.cassandraTable[Subject](settings("keyspaceDBpediaTable"), settings("dBpediaTable"))
		val wikidata = sc.cassandraTable[Subject](settings("keyspaceWikiDataTable"), settings("wikiDataTable"))
		val goldStandard = sc.cassandraTable[(UUID, UUID)](
			settings("keyspaceGoldStandardTable"),
			settings("goldStandardTable"))
		List(dbpedia, wikidata).toAnyRDD() ::: List(goldStandard).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[FeatureEntry]()
			.head
			.saveToCassandra(settings("keyspaceFeatureTable"), settings("featureTable"))
	}
	// $COVERAGE-ON$

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = input.head.asInstanceOf[RDD[Subject]]
		val wikidata = input(1).asInstanceOf[RDD[Subject]]
		val goldStandard = input(2).asInstanceOf[RDD[(UUID, UUID)]]

		val blocks = Blocking.blocking(dbpedia, wikidata, this.blockingSchemes)
		val features = findDuplicates(blocks.values, sc)
		val labeledPoints = labelFeature(features, goldStandard)
		List(labeledPoints).toAnyRDD()
	}

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
	  * @return tuple of Subjects with their score
	  */
	def findDuplicates(blocks: RDD[Block], sc: SparkContext): RDD[FeatureEntry] = {
		val confBroad = sc.broadcast(scoreConfigSettings)
		blocks
			.flatMap(_.crossProduct())
			.map { case (subject1, subject2) => createFeature(subject1, subject2, confBroad.value) }
	}
}
