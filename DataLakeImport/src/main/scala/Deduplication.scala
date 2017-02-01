package DataLake

import java.util.{Date, UUID}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import scala.xml.XML
import scala.collection.mutable.ListBuffer

class TestClassifier extends Classifier {
	override def execute(subject1: Subject, subject2: Subject): Double = {
		if(subject1.name.getOrElse("") == subject2.name.getOrElse(""))
			1.0
		else
			0.0
	}
}

case class scoreConfig[A, B <: SimilarityMeasure[A]](key: String, similarityMeasure: B, weight: Double) {
	override def equals(x: Any) = x match {
		case that: scoreConfig[A, B] => that.key == this.key && that.similarityMeasure.equals(this.similarityMeasure) && that.weight == this.weight
		case _ => false
	}
}

object Deduplication {
	val confidenceThreshold = 0.7
	val classifiers = List((new TestClassifier(), 1.0))
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

		val duplicates = findDuplicates(joined)
			.map(_.productIterator.mkString(","))
			.saveAsTextFile(appName + "_" + System.currentTimeMillis / 1000)

		sc
			.parallelize(List((version.version, version.timestamp, version.datasources, version.program)))
			.saveToCassandra(keyspace, versionTable, SomeColumns("version", "timestamp", "datasources", "program"))
	}

	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	def findDuplicates(joinedSubjects: RDD[(String, (Subject, Subject))]): RDD[(UUID, String, UUID, String, Double)] = {
		joinedSubjects
		  .map { case (key, (subject1, subject2)) =>
				Tuple5(subject1.id, subject1.name.getOrElse(""), subject2.id, subject2.name.getOrElse(""), score(subject1, subject2))
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

	def executeClassifiers(subject1: Subject, subject2: Subject): Double = {
		var confidenceSum = 0.0
		var weightSum = 0.0
		for((classifier, weight) <- classifiers) {
			val confidence = classifier.execute(subject1, subject2)

			confidenceSum += confidence * weight
			weightSum += weight
		}

		confidenceSum /= weightSum
		confidenceSum
	}

	def score(subject1: Subject, subject2: Subject): Double = {
		val list = ListBuffer.empty[Double]
		for(item <- config) {
			list += item.similarityMeasure.score(subject1.get(item.key), subject2.get(item.key)) * item.weight
		}
		list.foldLeft(0.0)((b, a) => b+a) / config.length
	}

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
}
