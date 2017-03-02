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
  *
  * @param key the attribute of a Subject to be compared
  * @param similarityMeasure the type of similarity measure for the comparing
  * @param weight the weight of the calculation
  * @tparam A type of the attribute
  * @tparam B type of the similarity measure
  */
case class scoreConfig[A, B <: SimilarityMeasure[A]](
	key: String,
	similarityMeasure: B,
	weight: Double)

/**
  * Possible duplication object
  *
  * @param subject1 id of subject1
  * @param subject2 id of subject 2
  * @param confidence similarity score
  */
case class possibleDuplicate(
	subject1: UUID,
	subject2: UUID,
	confidence: Double)
{
	def isConfident(): Boolean = this.confidence > Deduplication.confidenceThreshold
}

/**
  * Deduplication object for finding an handling duplicates in the database
  */
object Deduplication {
	val confidenceThreshold = 0.1
	var config = List[scoreConfig[_, _ <: SimilarityMeasure[_]]]()
	val appName = "Deduplication"
	val dataSources = List("") // TODO
	val keyspace = "datalake"
	val inputTable = "subject"
	val stagingTable = "staging"
	val versionTable = "version"
	val dataTypes = Map[String, SimilarityMeasure[_]](
		"ExactMatchString" -> ExactMatchString,
		"MongeElkan" -> MongeElkan,
		"Jaccard" -> Jaccard,
		"DiceSorensen" -> DiceSorensen,
		"Jaro" -> Jaro,
		"JaroWinkler" -> JaroWinkler,
		"N-Gram" -> NGram,
		"Overlap" -> Overlap)

	def main(args: Array[String]): Unit = {
		var doMerge = false
		if(args.length > 0) {
			config = parseConfig(args(0))
		} else {
			println("Usage config_path doMerge")
		}

		if(args.length > 1) {
			doMerge = args(1) == "merge"
		}

		val conf = new SparkConf()
			.setAppName(appName)
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)

		val subjects = sc.cassandraTable[Subject](keyspace, inputTable)
		val version = makeTemplateVersion()

		val blocksRDD = if(doMerge) {
			generateBlocks(subjects, sc.cassandraTable[Subject](keyspace, stagingTable), sc)
		} else {
			generateBlocks(subjects, sc)
		}
		val duplicates = findDuplicates(blocksRDD, this.config)

		duplicates.foreach(x => x._2.foreach(println))

		sc
			.parallelize(List((
				version.version,
				version.timestamp,
				version.datasources,
				version.program)
			))
			.saveToCassandra(
				keyspace,
				versionTable,
				SomeColumns("version", "timestamp", "datasources", "program")
			)
	}

	/**
	  * Creates a new version for the import
	  *
	  * @return a version reproducing to the details of the import
	  */
	def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	/**
	  * Compares to subjects regarding the configuration
	  *
	  * @param subject1 subjects to be compared to subject2
	  * @param subject2 subjects to be compared to subject2
	  * @param config configuration of the similarity measures for the comparison
	  * @return the similarity score of the subjects
	  */
	def compare(
		subject1: Subject,
		subject2: Subject,
		config: List[scoreConfig[_, _ <: SimilarityMeasure[_]]]
	): Double =
	{
		val list = ListBuffer[Double]()
		for(item <- config) {
			list += item.similarityMeasure
				.compare(subject1.get(item.key), subject2.get(item.key)) * item.weight
		}
		list.sum / config.length
	}

	/**
	  * Reads the configuration from a xml file
	  *
	  * @param path the path where the config.xml is located
	  * @return a list containing scoreConfig entities parsed from the xml file
	  */
	def parseConfig(path: String): List[scoreConfig[_, _ <: SimilarityMeasure[_]]] = {
		val xml = XML.loadFile(path)
		val config = (xml \\ "items" \ "item" ).toList

		config
			.map(item => Tuple3(
				(item \ "attribute").text,
				(item \ "similartyMeasure").text,
				(item \ "weight").text))
			.map { case (attribute, similarityMeasure, weight) =>
				scoreConfig[String, SimilarityMeasure[String]](
					attribute,
					dataTypes.getOrElse(similarityMeasure, ExactMatchString)
						.asInstanceOf[SimilarityMeasure[String]],
					weight.toDouble)
			}
	}

	/**
	  * Collects all different industries of the subjects given
	  *
	  * @param subjects Subjects from whom to collect the industries
	  * @return List of all the industries the subjects work in
	  */
	def collectIndustries(subjects: RDD[Subject]): List[String] = {
		subjects
			.map(x => x.properties.getOrElse("branche", List("uncategorized")))
			.reduce((x, y) => (x ::: y).distinct)
	}

	/**
	  * Creates blocks for the different industries
	  *
	  * @param subjects RDD to peform the blocking on
	  * @return RDD of blocks. One block for each industry
	  * containing the Subjects categorized as this industry
	  */

	def generateBlocks(
		subjects: RDD[Subject],
		sc: SparkContext
	): RDD[(String, Iterable[Subject])] =
	{
		val industries = collectIndustries(subjects)
		sc.parallelize(industries.map(x =>
			(x, subjects.filter(subject =>
				subject
					.properties
					.getOrElse("branche", List("uncategorized"))
					.contains(x))
					.collect
					.toList)))
	}

	/**
	  * Creates blocks for the different industries
	  *
	  * @param subjects RDD to peform the blocking on
	  * @param stagingSubjects RDD to peform the blocking on
	  * @return RDD of blocks. One block for each industry containing the
	  * Subjects categorized as this industry
	  */

	def generateBlocks(
		subjects: RDD[Subject],
		stagingSubjects: RDD[Subject],
		sc: SparkContext
	): RDD[(String, Iterable[Subject])] =
	{
		val subjectBlocks = generateBlocks(subjects, sc)
		val stagingBlocks = generateBlocks(stagingSubjects, sc)
		subjectBlocks
			.fullOuterJoin(stagingBlocks)
			.map {
				case (branche, (None, None)) => (branche, List())
				case (branche, (Some(x), None)) => (branche, x.toList)
				case (branche, (None, Some(x))) => (branche, x.toList)
				case (branche, (Some(x), Some(y))) => (branche, x.toSet ++ y.toSet.toList)
			}
	}

	/**
	  * Creates a list by pairing all items without duplicates
	  *
	  * @param list List of items to pair
	  * @param acc Accumulator
	  * @tparam A Type of list
	  * @return List of pairs of all elements from the original list without containing duplicates
	  */
	def makePairs[A](list: List[A], acc: List[(A, A)] = List[(A, A)]()): List[(A, A)] = list match {
		case Nil => acc
		case x::Nil => acc
		case x::xs => makePairs(xs, acc ::: list.map((x,_)).filter(x => x._1 != x._2))
	}

	/**
	  * Calculates a similarity score for each pair of Subjects of the same block
	  *
	  * @param blocks Block containing the list of similiar Subjects
	  * @param config Configuration for score calculation
	  * @return RDD containing a list of triples (subject1, subject2, similarity score)
	  * for each block
	  */
	def findDuplicates(
		blocks: RDD[(String, Iterable[Subject])],
		config: List[scoreConfig[_,_ <: SimilarityMeasure[_]]]
	): RDD[(String, Iterable[possibleDuplicate])] =
	{
		blocks
			.map {
				case (industry, subjects) => (industry, makePairs(subjects.toList))
			}.map {
				case (industry, subjectPairs) => (industry, subjectPairs.map(
					pair => possibleDuplicate(
						pair._1.id, pair._2.id, compare(pair._1, pair._2, config))))
			}.map {
				case (industry, possibleDuplicates) =>
					(industry, possibleDuplicates.filter(_.isConfident()))
			}
	}
}
