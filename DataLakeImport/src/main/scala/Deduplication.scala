package DataLake

import java.util.{Date, UUID}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import scala.xml.XML
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
	* Reproduces a configuration for a comparison of two Subjects
	* @param key the attribute of a Subject to be compared
	* @param similarityMeasure the type of similarity measure for the comparing
	* @param weight the weight of the calculation
	* @tparam A type of the attribute
	* @tparam B type of the similarity measure
	*/
case class scoreConfig[A, B <: SimilarityMeasure[A]](
		key: String,
		similarityMeasure: B,
		weight: Double,
		scale: Int = 1) {

	override def equals(obj: Any): Boolean = obj match {
		case that: scoreConfig[A, B] => that.key == this.key &&
			that.similarityMeasure.equals(this.similarityMeasure) &&
			that.weight == this.weight
		case _ => false
	}
}

/**
	* Abstract class containing generic methods for the deduplication
	* @param confidence minimum confidence for two subjects to be identified as duplicates
	* @param appName name of the deduplication
	* @param dataSources sources where the new data is retrieved from
	*/
class Deduplication (
	val confidence: Double,
	val appName: String,
	val dataSources: List[String],
	val configFile: String
){
	var config = List[scoreConfig[_,_ <: SimilarityMeasure[_]]]()
	val settings = mutable.Map[String, String]()

	def apply() {}

	/**
	* Creates a list by pairing all subjects without order
	* @param list List of subjects
	* @param acc accumulator
	* @return list of paired subjects
	*/
	def makePairs(list: List[Subject], acc: List[(Subject, Subject)] = List[(Subject, Subject)]()): List[(Subject, Subject)] = list match {
		case Nil => acc
		case x::Nil => acc
		case x::xs => makePairs(xs, acc ::: list.map((x,_)).filter(x => x._1 != x._2))
	}

	/**
	* Compares to subjects regarding the configuration
	* @param subject1 subjects to be compared to subject2
	* @param subject2 subjects to be compared to subject2
	* @param config configuration of the similarity measures for the comparison
	* @return the similarity score of the subjects
	*/
	def compare(subject1: Subject, subject2: Subject, config: List[scoreConfig[_,_ <: SimilarityMeasure[_]]], scale: Int): (Subject, Subject, Double, Double) = {
		val list = ListBuffer.empty[Double]
		for(item <- config) {
			list += item.similarityMeasure.compare(subject1.get(item.key), subject2.get(item.key), scale) * item.weight
		}
		(subject1, subject2, list.sum / config.length, scale)
	}

	/**
	* Reads the configuration from a xml file
	* @param path the path where the config.xml is located
	* @return a list containing scoreConfig entities parsed from the xml file
	*/
	def parseConfig(path: String): List[scoreConfig[_,_ <: SimilarityMeasure[_]]] = {
		val xml = XML.loadFile(path)
		val measures = (xml \\ "deduplictaion" \ "simMeasurements" \ "item").toList


		val configSettings = (xml \\ "deduplication" \ "sourceSettings" )
		settings("keyspace") = (configSettings \ "keyspace").text
		settings("stagingTable") = (configSettings \ "stagingTable").text
		settings("subjectTable") = (configSettings \ "subjectTable").text
		settings("duplicatesTable") = (configSettings \ "duplicatesTable").text
		settings("versionTable") = (configSettings \ "versionTable").text

		measures
			.map(item =>
				((item \ "attribute").text,
					(item \ "similartyMeasure").text,
					(item \ "weight").text,
					(item \ "scale").text))
			.map { case (attribute, similarityMeasure, weight, scale) =>
				scoreConfig[String, SimilarityMeasure[String]](
					attribute,
					Deduplication.dataTypes.getOrElse(similarityMeasure, ExactMatchString)
						.asInstanceOf[SimilarityMeasure[String]],
					weight.toDouble,
					scale.toInt)
			}
	}

	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	/**
	* Partitions the subjects in buckets
	* @param subjects subjects to be blocked
	* @return tuple containing a describing String and an Iterable containing the Subjects
	*/
	def blocking(subjects: RDD[Subject]): RDD[(String, Iterable[Subject])] = {
		subjects
			.groupBy(subject => subject.id.toString.hashCode().%(500).toString )
	}
	/**
		* Partitions the subjects in buckets
		* @param subjects subjects to be blocked
		* @param stagings stagings to be blocked
		* @return tuple containing a describing String and an Iterable containing the Subjects
		*/
	def blocking(subjects: RDD[Subject], stagings: RDD[Subject]): RDD[(String, Iterable[Subject])] = {
		val subjectBlocks = blocking(subjects)
		val stagingBlocks = blocking(stagings)
		subjectBlocks
			.fullOuterJoin(stagingBlocks)
			.map{
				case (description, (None, None)) => (description, List())
				case (description, (x, None)) => (description, x.get.toList)
				case (description, (None, x)) => (description, x.get.toList)
				case (description, (x, y)) => (description, x.get.toSet.++(y.get.toSet).toList)
			}
	}

	/**
		* Divides the subjects into buckets respecting an entry in the properties
		* @param subjects subjects to be blocked
		* @param key key of the properties to be partitioned by
		* @param sc Spark Context
		* @return tuple containing the value and a list of all subjects containing the value
		*/
	def blockingByKey(subjects: RDD[Subject], key: String, sc: SparkContext): RDD[(String, Iterable[Subject])] = {
		val values = subjects
		  .map(subject => {
				subject
					.properties
				  .getOrElse(key, List("uncategorized"))
			})
		  .reduce(_.:::(_).distinct)

		var blocks = values
		  .map(value => {
				(value, subjects
					.filter(subject => {
						subject
							.properties
							.getOrElse(key, List("uncategorized"))
							.contains(value)
					})
					.collect()
					.toList
			)})

		if (blocks.contains(("uncategorized", _))) {
			val uncategorizedSubjects = blocks
				.filter(_._1 == "uncategorized")
				.head
			  ._2

			blocks = blocks
				.map(block => (block._1, block._2 ::: uncategorizedSubjects))
				.filter(_._1 != "uncategorized")
		}

		sc.parallelize[(String, Iterable[Subject])](blocks)
	}

	/**
	* Finds the duplicates of each block
	* @param blocks buckets where the duplicates are searched in
	* @return tuple of Subjects and their score whose score is greater than the confidence threshold
	*/
	def findDuplicates(blocks: RDD[(String, Iterable[Subject])]): List[(Subject, Subject)] = {
		blocks
			.map{
				case (description, subjects) => makePairs(subjects.toList)
			}
		  .map {
				list => list
					.map(pair => compare(pair._1, pair._2, this.config))
					.filter(pair => pair._3 > this.confidence)
					.map(pair => (pair._1, pair._2))
			}
			.reduce(_ ::: _)
	}

	/**
		* Merging two duplicates in one subject
		* @param duplicates tuple of Subject
		* @return subjects containing the merged information of both duplicates
		*/
	def merging(duplicates: RDD[(Subject, Subject)], version: Version): RDD[Subject] = {
		duplicates
		  .map(x => {
				val sm = new SubjectManager(x._1, version)
				sm.addProperties(x._2.properties)
				x._1
			})
	}

	def deduplication(merge: Boolean = false): Unit = {
		val conf = new SparkConf()
		  .setAppName(this.appName)
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val subjects = sc.cassandraTable[Subject](settings("keyspace"), settings("subjectTable"))
		val stagings = if(merge) {sc.cassandraTable[Subject](settings("keyspace"), settings("subjectTable")) } else { null }
		val version = makeTemplateVersion()

		val blocks = if(merge) blocking(subjects, stagings) else blocking(subjects)
		val duplicates = sc.parallelize(findDuplicates(blocks))
		val merged = merging(duplicates, version)

		merged.saveToCassandra(settings("keyspace"), settings("duplicatesTable"))
	}
}

object Deduplication {
	val dataTypes = Map[String, SimilarityMeasure[_]](
	"ExactMatchString" -> ExactMatchString,
	"MongeElkan" -> MongeElkan,
	"Jaccard" -> Jaccard,
	"DiceSorensen" -> DiceSorensen,
	"Jaro" -> Jaro,
	"JaroWinkler" -> JaroWinkler,
	"N-Gram" -> NGram,
	"Overlap" -> Overlap)
}
