package de.hpi.ingestion.deduplication

import java.util.{Date, UUID}

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.similarity._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

import scala.xml.XML
import scala.collection.mutable
import scala.io.Source

/**
  * Case class for corresponding duplicatecandidates cassandra table.
  * @param id unique UUID used as primary key
  * @param subject_id UUID of subject in the existing subject table
  * @param subject_name name of subject in the existing subject table
  * @param duplicate_id UUID of duplicate subject
  * @param duplicate_name name of duplicate subject
  * @param duplicate_table source table of duplicate
  */
case class DuplicateCandidate(
	id: UUID,
	subject_id: UUID,
	subject_name: Option[String] = None,
	duplicate_id: UUID,
	duplicate_name: Option[String] = None,
	duplicate_table: String)

/**
  * Reproduces a configuration for a comparison of two Subjects
  * @param key the attribute of a Subject to be compared
  * @param similarityMeasure the similarity measure used for the comparison
  * @param weight the weight of the result
  * @param scale specifies the n-gram if the similarity measure has one
  * @tparam A type of the attribute
  * @tparam B type of the similarity measure
  */
case class scoreConfig[A, B <: SimilarityMeasure[A]](
	key: String,
	similarityMeasure: B,
	weight: Double,
	scale: Int = 1)

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
	var config = List[scoreConfig[String, SimilarityMeasure[String]]]()
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

				scoreConfig[String, SimilarityMeasure[String]](
					attribute,
					Deduplication.dataTypes
						.getOrElse(simMeasure, ExactMatchString)
						.asInstanceOf[SimilarityMeasure[String]],
					weight.toDouble,
					scale.toInt)
			}
	}

	/**
	  * Creates timestamp and TimeUUID for versioning.
	  * @return New current Version object.
	  */
	def makeTemplateVersion(): Version = {
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	/**
	  * Uses input blocking scheme to generate blocks of subjects.
	  * @param subjects RDD to perform the blocking on.
	  * @param blockingScheme blocking scheme to use to generate keys.
	  * @return subjects grouped by blocking keys
	  */
	def blocking(
		subjects: RDD[Subject],
		blockingScheme: BlockingScheme
	): RDD[(List[String], List[Subject])] = {
		subjects
			.map(subject => (blockingScheme.generateKey(subject), List(subject)))
			.reduceByKey(_ ++ _)
	}

	/**
	  * Creates blocks based on the input blocking scheme
	  * @param subjects RDD to perform the blocking on
	  * @param stagingSubjects RDD to perform the blocking on
	  * @param blockingScheme blocking scheme to use to generate keys.
	  * @return RDD of blocks.
	  */
	def blocking(
		subjects: RDD[Subject],
		stagingSubjects: RDD[Subject],
		blockingScheme: BlockingScheme = new SimpleBlockingScheme()
	): RDD[(List[String], List[Subject])] = {
		val subjectBlocks = blocking(subjects, blockingScheme)
		val stagingBlocks = blocking(stagingSubjects, blockingScheme)
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

		// from http://stackoverflow.com/questions/14740199/cross-product-in-scala
		implicit class Crossable[X](xs: Traversable[X]) {
			def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
		}

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
	  * Merging two duplicates in one subject
	  * @param duplicates tuple of Subject
	  * @return subjects containing the merged information of both duplicates
	  */
	def merging(duplicates: RDD[(Subject, Subject)], version: Version): RDD[Subject] = {
		duplicates
			.map { x =>
				val sm = new SubjectManager(x._1, version)
				sm.addProperties(x._2.properties)
				x._1
			}
	}

	/**
	  * Starts the deduplication process and writes the result to Cassandra.
	  * @param merge boolean indicating whether or not the duplicates should be merged
	  */
	def run(merge: Boolean = false): Unit = {
		val conf = new SparkConf()
		  .setAppName(this.appName)
		val sc = new SparkContext(conf)
		parseConfig()
		val subjects = sc.cassandraTable[Subject](settings("keyspace"), settings("subjectTable"))
		val blockingScheme = new SimpleBlockingScheme()
		val blocks = if(merge) {
			val staging = sc.cassandraTable[Subject](settings("keyspace"), settings("stagingTable"))
			blocking(subjects, staging, blockingScheme)
		} else {
			blocking(subjects, blockingScheme)
		}

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
		"Overlap" -> Overlap)
}
