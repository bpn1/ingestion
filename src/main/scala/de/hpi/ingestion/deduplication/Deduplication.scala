package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.implicits.CollectionImplicits._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

import scala.xml.XML
import scala.collection.mutable
import scala.io.Source

/**
  * Abstract class containing generic methods for the deduplication
  * @param confidence minimum confidence for two subjects to be identified as duplicates
  * @param appName name of the deduplication
  * @param dataSources sources where the new data is retrieved from
  * @param configFile name of config file in resource folder
  */
class Deduplication(
	val confidence: Double,
	val appName: String,
	val dataSources: List[String],
	val configFile: Option[String] = None
) extends Serializable {
	var config = List[ScoreConfig[String, SimilarityMeasure[String]]]()
	val settings = mutable.Map[String, String]()

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
		settings("keyspace") = (configSettings \ "keyspace").text
		settings("stagingTable") = (configSettings \ "stagingTable").text
		settings("subjectTable") = (configSettings \ "subjectTable").text
		settings("duplicatesTable") = (configSettings \ "duplicatesTable").text
		settings("versionTable") = (configSettings \ "versionTable").text

		config = (xml \\ "config" \ "simMeasurements" \ "feature")
			.toList
			.map { feature =>
				val attribute = (feature \ "attribute").text
				val simMeasure = (feature \ "similartyMeasure").text
				val weight = (feature \ "weight").text
				val scale = (feature \ "scale").text

				ScoreConfig[String, SimilarityMeasure[String]](
					attribute,
					Deduplication.dataTypes
						.getOrElse(simMeasure, ExactMatchString)
						.asInstanceOf[SimilarityMeasure[String]],
					weight.toDouble,
					scale.toInt)
			}
	}

	/**
	  * Uses input blocking scheme to generate blocks of subjects.
	  * @param subjects RDD to perform the blocking on.
	  * @param blockingSchemes blocking schemes to use to generate keys.
	  * @return subjects grouped by blocking keys
	  */
	def blocking(
		subjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme]
	): RDD[(List[List[String]], List[Subject])] = {
		subjects
			.map(subject => (blockingSchemes.map(_.generateKey(subject)), List(subject)))
			.reduceByKey(_ ++ _)	// ToDo: #182, better reduce function
	}

	/**
	  * Creates blocks based on the input blocking scheme
	  * @param subjects RDD to perform the blocking on
	  * @param stagingSubjects RDD to perform the blocking on
	  * @param blockingSchemes blocking schemes to use to generate keys.
	  * @return RDD of blocks.
	  */
	def blocking(
		subjects: RDD[Subject],
		stagingSubjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme]
	): RDD[(List[List[String]], List[Subject])] = {
		val subjectBlocks = blocking(subjects, blockingSchemes)
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes)
		subjectBlocks
			.fullOuterJoin(stagingBlocks)
			.map {
				case (key, (Some(x), None)) => (key, x)
				case (key, (None, Some(x))) => (key, x)
				case (key, (Some(x), Some(y))) => (key, (x ++ y).distinct)
			}
		println(subjects.count.toString)
		subjectBlocks
	}

	/**
	  * Finds the duplicates of each block
	  * @param block List of Subjects where the duplicates are searched in
	  * @return tuple of Subjects whose score is greater or equal a given threshold this.confidence
	  */
	def findDuplicates(block: List[Subject]): List[(Subject, Subject)] = {
		block
			.cross(block)
			.filter(tuple => tuple._1 != tuple._2)
			.map(tuple => Set(tuple, tuple.swap))
			.toList
			.distinct
			.map(_.head)
			.filter(tuple => compare(tuple._1, tuple._2) >= this.confidence)
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
		val subject_manager1 = new SubjectManager(subject1, version)
		val subject_manager2 = new SubjectManager(subject2, version)

		subject_manager1.addRelations(Map(subject2.id -> relations))
		subject_manager2.addRelations(Map(subject1.id -> relations))
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
				Map("type" -> "isDuplicate", "confidence" -> (scoredDuplicatePair._3).toString),
				version
			)
		}
	}

	/**
	  * Executes methods on the blocked data to gain information.
	  *
	  * @param blocks  Input data, partitioned in blocks.
	  * @param comment Note to add to the evaluated data.
	  * @return Blocks with size-value and comment.
	  */
	def evaluateBlocks(
		blocks: RDD[(List[List[String]], List[Subject])],
		comment: String
	): BlockEvaluation = {
		val dataMap = blocks
			.map(block => (block._1.map(_.mkString(" ")), block._2.size))
			.sortBy(_._2, false)
			.collect()
			.toMap
		BlockEvaluation(data = dataMap, comment = Option(comment))
	}

	/**
	  * Starts the deduplication process and writes the result to Cassandra.
	  * @param blockingSchemes List of blocking schemes to be used on the data
	  * @param merge boolean indicating whether or not the duplicates should be merged
	  */
	def run(
		blockingSchemes:  List[BlockingScheme] = List(new SimpleBlockingScheme),
		merge: Boolean = false
	): Unit = {
		val conf = new SparkConf()
			.setAppName(this.appName)
		val sc = new SparkContext(conf)
		parseConfig()
		val subjects = sc.cassandraTable[Subject](settings("keyspace"), settings("subjectTable"))
		val blocks = if(merge) {
			val staging = sc.cassandraTable[Subject](settings("keyspace"), settings("stagingTable"))
			blocking(subjects, staging, blockingSchemes)
		} else {
			blocking(subjects, blockingSchemes)
		}

		val blockEvaluation = evaluateBlocks(blocks, this.appName)
		sc
			.parallelize(Seq(blockEvaluation))
			.saveToCassandra(settings("keyspace"), settings("statsTable"))

		blocks
			.flatMap(block => findDuplicates(block._2))
		    .map { case (subject1, subject2) =>
				DuplicateCandidate(
					UUIDs.random(),
					subject1.id,
					subject1.name,
					subject2.id,
					subject2.name,
					settings("stagingTable"))
			}.saveToCassandra(settings("keyspace"), settings("duplicatesTable"))
	}
}

object Deduplication {
	val dataTypes: Map[String, SimilarityMeasure[_]] = Map(
		"ExactMatchString" -> ExactMatchString,
		"MongeElkan" -> MongeElkan,
		"Jaccard" -> Jaccard,
		"DiceSorensen" -> DiceSorensen,
		"Jaro" -> Jaro,
		"JaroWinkler" -> JaroWinkler,
		"N-Gram" -> NGram,
		"Overlap" -> Overlap,
		"RoughlyEqualNumbers" -> RoughlyEqualNumbers
	)
}
