package de.hpi.ingestion.deduplication

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.deduplication.blockingschemes._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.xml.{Node, XML}

/**
  * Compares two groups of Subjects by first blocking with multiple Blocking Schemes, then compares all Subjects
  * in every block and filters all pairs below a given threshold.
  */
object Deduplication extends SparkJob {
	appName = "Deduplication"
	var config = Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]()
	var settings = Map[String, String]()
	val configFile: Option[String] = None
	val blockingSchemes = List[BlockingScheme](SimpleBlockingScheme("simple_scheme"))

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects and the staged Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = List(sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable")))
		val staging = List(sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable")))
		(subjects ++ staging).toAnyRDD()
	}

	/**
	  * Saves the duplicates in the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[DuplicateCandidates]()
			.head
			.saveToCassandra(settings("keyspaceDuplicatesTable"), settings("duplicatesTable"))
	}
	// $COVERAGE-ON$

	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	/**
	  * Blocks the Subjects and finds duplicates between them between the Subjects and staged Subjects.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val List(subjects, staging) = input.fromAnyRDD[Subject]()
		val blocks = Blocking.blocking(subjects, staging, blockingSchemes)
		val subjectPairs = findDuplicates(blocks.values, sc)
		val duplicates = createDuplicateCandidates(subjectPairs)
		List(duplicates).toAnyRDD()
	}

	/**
	  * Reads the configuration from an xml file.
	  * @return a list containing scoreConfig entities parsed from the xml file
	  */
	def parseConfig(): Unit = {
		val path = this.configFile.getOrElse("config.xml")
		val xml = XML.loadString(Source
			.fromURL(getClass.getResource(s"/$path"))
			.getLines()
			.mkString("\n"))

		val configSettings = xml \\ "config" \ "sourceSettings"
		settings = configSettings.head.child
			.collect {
				case node: Node if node.text.trim.nonEmpty => (node.label, node.text)
			}.toMap

		val attributes = xml \\ "config" \ "simMeasurements" \ "attribute"
		config = attributes.map { node =>
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

	/**
	  * Compares to subjects regarding the configuration.
	  * @param subject1 subjects to be compared to subject2
	  * @param subject2 subjects to be compared to subject1
	  * @return the similarity score of the subjects
	  */
	def compare(
		subject1: Subject,
		subject2: Subject,
		scoreConfigMap: Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]],
		scale: Int = 1
	): Double = {
		val weights = scoreConfigMap
			.values
			.flatMap(_.map(_.weight))
			.sum

		val scores = scoreConfigMap.flatMap { case (attribute, configs) =>
			val valueSubject1 = subject1.get(attribute)
			val valueSubject2 = subject2.get(attribute)
			configs.collect {
				case scoreConfig if valueSubject1.nonEmpty && valueSubject2.nonEmpty =>
					CompareStrategy(attribute)(valueSubject1, valueSubject2, scoreConfig)
			}
		}
		scores.sum / weights
	}

	/**
	  * Groups all found duplicates by the Subject whose duplicate they are and creates the corresponding
	  * DuplicateCandidates.
	  * @param subjectPairs RDD of Subject pairs containing all found duplicates
	  * @return RDD of grouped Duplicate Candidates
	  */
	def createDuplicateCandidates(subjectPairs: RDD[(Subject, Subject, Double)]): RDD[DuplicateCandidates] = {
		val stagingTable = this.settings("stagingTable")
		subjectPairs
			.map { case (subject1, subject2, score) =>
				(subject1.id, List((subject2, stagingTable, score)))
			}.reduceByKey(_ ::: _)
			.map(DuplicateCandidates.tupled)
	}

	/**
	  * Finds the duplicates of each block by comparing the Subjects and filtering all Subjects pairs below the
	  * threshold {@confidence}.
	  * @param blocks RDD of BLocks containing the Subjects that are compared
	  * @return tuple of Subjects with their score, which is greater or equal the given threshold.
	  */
	def findDuplicates(blocks: RDD[Block], sc: SparkContext): RDD[(Subject, Subject, Double)] = {
		val threshold = this.settings("confidence").toDouble
		val confBroad = sc.broadcast(this.config)
		blocks
			.flatMap(_.crossProduct())
			.map { case (subject1, subject2) =>
				(subject1, subject2, compare(subject1, subject2, confBroad.value))
			}.filter(_._3 >= threshold)
	}
}
