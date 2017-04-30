package de.hpi.ingestion.deduplication

import java.util.UUID
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.BlockingJob.{getClass, settings}
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import scala.collection.mutable
import scala.io.Source
import scala.xml.XML


object DeduplicationJob extends SparkJob {
	appName = "Deduplication"
	var config = List[ScoreConfig[String, SimilarityMeasure[String]]]()
	val settings = mutable.Map[String, String]()
	val configFile: Option[String] = None

	val dataTypes: Map[String, SimilarityMeasure[_]] = Map(
		"ExactMatchString" -> ExactMatchString,
		"MongeElkan" -> MongeElkan,
		"Jaccard" -> Jaccard,
		"DiceSorensen" -> DiceSorensen,
		"Jaro" -> Jaro,
		"JaroWinkler" -> JaroWinkler,
		"N-Gram" -> NGram,
		"Overlap" -> Overlap,
		"EuclidianDistance" -> EuclidianDistance,
		"RoughlyEqualNumbers" -> RoughlyEqualNumbers
	)

	// $COVERAGE-OFF$

	/**
	  * Loads the Subjects and if needed the staged Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {

	}
	// $COVERAGE-ON$

	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val blocks = input.fromAnyRDD[DuplicateCandidates]()

		val subjectPairs = blocks.map { blocking =>
			blocking.flatMap(block => findDuplicates(block._2))
		}.reduce(_.union(_))

		val duplicates = createDuplicateCandidates(subjectPairs)
		List(duplicates).toAnyRDD()
	}

	/**
	  * Reads the configuration from a xml file
	  * @return a list containing scoreConfig entities parsed from the xml file
	  */
	def parseConfig(): Unit = {
		val path = this.configFile.getOrElse("config.xml")
		val xml = XML.loadString(Source
			.fromURL(getClass.getResource(s"/$path"))
			.getLines()
			.mkString("\n"))

		val configSettings = xml \\ "config" \ "sourceSettings"
		for(node <- configSettings.head.child if node.text.trim.nonEmpty)
			settings(node.label) = node.text

		config = (xml \\ "config" \ "simMeasurements" \ "feature")
			.toList
			.map { feature =>
				val attribute = (feature \ "attribute").text
				val simMeasure = (feature \ "similarityMeasure").text
				val weight = (feature \ "weight").text
				val scale = (feature \ "scale").text

		ScoreConfig[String, SimilarityMeasure[String]](
			attribute,
			this.dataTypes
				.getOrElse(simMeasure, ExactMatchString)
				.asInstanceOf[SimilarityMeasure[String]],
			weight.toDouble,
			scale.toInt)
		}
	}

	/**
	  * Compares to subjects regarding the configuration
	  * @param subject1 subjects to be compared to subject2
	  * @param subject2 subjects to be compared to subject2
	  * @return the similarity score of the subjects
	  */
	def compare(
		subject1: Subject,
		subject2: Subject,
		scale: Int = 1
	): Double = {
		val scoreList = this.config
			.map { feature =>
				val valueSub1 = subject1.get(feature.key).headOption
				val valueSub2 = subject2.get(feature.key).headOption
				if(valueSub1.isDefined && valueSub2.isDefined) {
					val score = feature
						.similarityMeasure
						.compare(valueSub1.get, valueSub2.get, scale)
					score * feature.weight
				} else {
					-1.0
				}
				}.filter(_ != -1.0)
		scoreList.sum / Math.max(1, scoreList.length)
	}

	/**
	  * Creates candidates for the Deduplication for each element from the output of findDuplicates
	  * @param subjectPairs Pairs of subjects for all possible duplicates
	  * @return all actual candidates to save to the cassandra
	  */
	def createDuplicateCandidates(subjectPairs: RDD[(Subject, Subject, Double)]): RDD[DuplicateCandidates] = {
		subjectPairs
			.map { case (subject1, subject2, score) =>
				(subject1.id, List((subject2, settings("stagingTable"), score)))
				  }.reduceByKey(_ ::: _)
			.map(DuplicateCandidates.tupled)
	}

	/**
	  * Finds the duplicates of each block
	  * @param block List of Subjects where the duplicates are searched in
	  * @return tuple of Subjects with it's score, which is greater or equal a given threshold this.confidence
	  */
	def findDuplicates(block: List[Subject]): List[(Subject, Subject, Double)] = {
		block
			.cross(block)
			.filter(tuple => tuple._1 != tuple._2)
			.map(tuple => Set(tuple, tuple.swap))
			.toList
			.distinct
			.map(_.head)
			.map(tuple => (tuple._1, tuple._2, compare(tuple._1, tuple._2)))
			.filter(tuple => tuple._3 >= settings("confidence").toDouble)
	}

	/**
	  * Add symmetric relations between two subject nodes, abstraction from SubjectManagers
	  * addRelations method
	  * @param subject1 UUID of a given node
	  * @param subject2 UUID of another given node
	  * @param relations A set of attributes of keys and values describing the relation,
	  * typically a 'type' value should be passed
	  * @param version Current transactional id handle
	  */
	def addSymRelation(
		subject1: Subject,
		subject2: Subject,
		relations: Map[String,String],
		version: Version
	): Unit = {
		val subjectManager1 = new SubjectManager(subject1, version)
		val subjectManager2 = new SubjectManager(subject2, version)

		subjectManager1.addRelations(Map(subject2.id -> relations))
		subjectManager2.addRelations(Map(subject1.id -> relations))
	}

	/**
	  * Merging two duplicates in one subject
	  * @param duplicates tuple of Subject
	  * @return subjects containing the merged information of both duplicates
	  */
	def buildDuplicatesSCC(duplicates: List[(Subject, Subject, Double)], version: Version): Unit = {

		duplicates.foreach { scoredDuplicatePair =>
			addSymRelation(
				scoredDuplicatePair._1,
				scoredDuplicatePair._2,
				Map("type" -> "isDuplicate", "confidence" -> scoredDuplicatePair._3.toString),
				version
			)
		}
	}
}
