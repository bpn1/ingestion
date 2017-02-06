package DataLake

import java.util.{Date, UUID}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import scala.xml.XML
import scala.collection.mutable.ListBuffer

/**
	* Reproduces a configuration for a comparison of two Subjects
	* @param key the attribute of a Subject to be compared
	* @param similarityMeasure the type of similarity measure for the comparing
	* @param weight the weight of the calculation
	* @tparam A type of the key
	* @tparam B type of the similarity measure
	*/
case class scoreConfig[A, B <: SimilarityMeasure[A]](key: String, similarityMeasure: B, weight: Double) {
	override def equals(obj: Any): Boolean = obj match {
		case that: scoreConfig[A, B] => that.key == this.key && that.similarityMeasure.equals(this.similarityMeasure) && that.weight == this.weight
		case _ => false
	}
}

/**
	* Deduplikation object for finding an handling duplicates in the database
	*/
object Deduplication {
	val confidenceThreshold = 0.7
	var config = List[scoreConfig[_,_ <: SimilarityMeasure[_]]]()
	val appName = "Deduplication"
	val dataSources = List("") // TODO
	val keyspace = "datalake"
	val inputTable = "subject"
	val stagingTable = "staging"
	val versionTable = "version"

	def main(args: Array[String]): Unit = {
		var doMerge = false
		if(args.length > 0)
			config = parseConfig(args(0))
		else
			println("Usage config_path doMerge")
		if(args.length > 1)
			doMerge = args(1) == "merge"

		val conf = new SparkConf()
			.setAppName(appName)
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val subjects = sc.cassandraTable[Subject](keyspace, inputTable)
		val version = makeTemplateVersion()

		var joined: RDD[(String, (Subject, Subject))] = null
		if(doMerge) {
			val newSubjects = sc.cassandraTable[Subject](keyspace, stagingTable)
			joined = joinForMerge(newSubjects, subjects)
		} else {
			joined = joinForLink(subjects)
		}

		val duplicates = findDuplicates(joined, config)
			.map(_.productIterator.mkString(","))
			.saveAsTextFile(appName + "_" + System.currentTimeMillis / 1000)

		sc
			.parallelize(List((version.version, version.timestamp, version.datasources, version.program)))
			.saveToCassandra(keyspace, versionTable, SomeColumns("version", "timestamp", "datasources", "program"))
	}

	/**
		* Createds a new version for the import
		* @return a version reproducing to the details of the import
		*/
	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	/**
		* Findes all duplicates with a similarity score higher than the confidence threshold
		* @param joinedSubjects a rdd containing all Subjects to be compared
		* @param config the configuration of similarity measures algorithm for comparing
		* @return all pairs of Subjects with a similarity score higher than the confidence threshold with the score
		*/
	def findDuplicates(joinedSubjects: RDD[(String, (Subject, Subject))], config: List[scoreConfig[_,_ <: SimilarityMeasure[_]]]): RDD[(UUID, String, UUID, String, Double)] = {
		joinedSubjects
		  .map { case (key, (subject1, subject2)) =>
				Tuple5(subject1.id, subject1.name.getOrElse(""), subject2.id, subject2.name.getOrElse(""), compare(subject1, subject2, config))
			}
		  .filter(x => x._5 > confidenceThreshold)
	}

	def joinForMerge(newSubjects: RDD[Subject], subjects: RDD[Subject]): RDD[(String, (Subject, Subject))] = {
		val keyedSubjects = subjects.keyBy(makePartitionKey)

		newSubjects
			.keyBy(makePartitionKey)
			.join(keyedSubjects)
	}

	def joinForLink(subjects: RDD[Subject]): RDD[(String, (Subject, Subject))] = {
		val keyedSubjects = subjects
			.keyBy(makePartitionKey)

		keyedSubjects
			.join(keyedSubjects)
	}

	// TODO better partition key
	def makePartitionKey(subject: Subject): String = {
		subject.name.getOrElse("")
	}

	/**
		* Compares to subjects regarding the configuration
		* @param subject1 subjects to be compared to subject2
		* @param subject2 subjects to be compared to subject2
		* @param config configuration of the similarity measures for the comparison
		* @return the similarity score of the subjects
		*/
	def compare(subject1: Subject, subject2: Subject, config: List[scoreConfig[_,_ <: SimilarityMeasure[_]]]): Double = {
		val list = ListBuffer.empty[Double]
		for(item <- config) {
			list += item.similarityMeasure.compare(subject1.get(item.key), subject2.get(item.key)) * item.weight
		}
		list.foldLeft(0.0)((b, a) => b+a) / config.length
	}

	/**
		* Reads the configuration from a xml file
		* @param path the path where the config.xml is located
		* @return a list containing scoreConfig entities parsed from the xml file
		*/
	def parseConfig(path: String): List[scoreConfig[_,_ <: SimilarityMeasure[_]]] = {
		val xml = XML.loadFile(path)
		val config = (xml \\ "items" \ "item" ).toList

		config
		  .map(node => Tuple3((node \ "key").text, (node \ "similartyMeasure").text, (node \ "weight").text))
		  .map{
				case (key, similarityMeasure, weight) => similarityMeasure match {
					case "ExactMathString" => scoreConfig[String, ExactMatchString.type](key, ExactMatchString, weight.toDouble)
					case "MongeElkan" => scoreConfig[String, MongeElkan.type](key, MongeElkan, weight.toDouble)
					case _ => scoreConfig[String, ExactMatchString.type](key, ExactMatchString, weight.toDouble)
			}}
	}

	/**
		* Creates blocks for the different industries
		* @param subjects RDD to peform the blocking on
		* @return RDD of blocks. One block for each industry containing the Subjects categorized as this industry
		*/
	def generateBlocks(subjects: RDD[Subject]): RDD[(String, Iterable[Subject])] = {
		subjects.groupBy(x => x.properties.getOrElse("branche", List("uncategorized")).head)
	}

	/**
		* Creates blocks for the different industries
		* @param subjects RDD to peform the blocking on
		* @param stagingSubjects RDD to peform the blocking on
		* @return RDD of blocks. One block for each industry containing the Subjects categorized as this industry
		*/
	def generateBlocks(subjects: RDD[Subject], stagingSubjects: RDD[Subject]): RDD[(String, Iterable[Subject])] = {
		val subjectBlocks = subjects.groupBy(x => x.properties.getOrElse("branche", List("uncategorized")).head)
		val stagingBlocks = stagingSubjects.groupBy(x => x.properties.getOrElse("branche", List("uncategorized")).head)
		subjectBlocks
			.fullOuterJoin(stagingBlocks)
		  .map{
				case (branche, (x, y)) => (branche, x.getOrElse(Iterable())++y.getOrElse(Iterable()))
			}
	}
}
