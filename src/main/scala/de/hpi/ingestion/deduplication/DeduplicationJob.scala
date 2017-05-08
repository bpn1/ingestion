package de.hpi.ingestion.deduplication

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.deduplication.blockingschemes._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.io.Source
import scala.xml.XML

/**
  * Compares two groups of Subjects by first blocking with multiple Blocking Schemes, then compares all Subjects
  * in every block and filters all pairs below a given threshold.
  */
object DeduplicationJob extends SparkJob {
	appName = "Deduplication"
	var config = mutable.Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]()
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
		val inputRDDs = input.fromAnyRDD[Subject]()
		val subjects = inputRDDs.head
		val staging = inputRDDs(1)
		val blockingSchemes = List(SimpleBlockingScheme("simple_scheme"))
		val blocks = BlockingJob.blocking(subjects, staging, blockingSchemes)
		val subjectPairs = findDuplicates(blocks.values)
		val duplicates = createDuplicateCandidates(subjectPairs)
		List(duplicates).toAnyRDD()
	}

	/**
	  * Reads the configuration from an xml file.
	  * @return a list containing scoreConfig entities parsed from the xml file
	  */
	def parseConfig(): Unit = {
		config.clear()
		val path = this.configFile.getOrElse("config.xml")
		val xml = XML.loadString(Source
			.fromURL(getClass.getResource(s"/$path"))
			.getLines()
			.mkString("\n"))

		val configSettings = xml \\ "config" \ "sourceSettings"
		for(node <- configSettings.head.child if node.text.trim.nonEmpty)
			settings(node.label) = node.text

		val attributes = (xml \\ "config" \ "simMeasurements" \ "attribute").toList
		for {
			node <- attributes
			key = (node \ "key").text
			feature <- (node \ "feature").toList
			similarityMeasure = (feature \ "similarityMeasure").text
			weight = (feature \ "weight").text.toDouble
			scale = (feature \ "scale").text.toInt
		}{
			val scoreConfig = ScoreConfig[String, SimilarityMeasure[String]](
				SimilarityMeasure.get[String](similarityMeasure),
				weight,
				scale)
			this.config(key) = config.getOrElse(key, Nil) :+ scoreConfig
		}
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
		scale: Int = 1
	): Double = {
		val weights = config
			.values
			.flatMap(_.map(_.weight))
			.sum

		val scores = for {
			(attribute, scoreConfigs) <- this.config
			valueSubject1 = subject1.get(attribute)
			valueSubject2 = subject2.get(attribute)
			if valueSubject1.nonEmpty && valueSubject2.nonEmpty
			scoreConfig <- scoreConfigs
		} yield CompareStrategy(attribute)(valueSubject1, valueSubject2, scoreConfig)

		scores.sum / weights
	}

	/**
	  * Groups all found duplicates by the Subject whose duplicate they are and creates the corresponding
	  * DuplicateCandidates.
	  * @param subjectPairs RDD of Subject pairs containing all found duplicates
	  * @return RDD of grouped Duplicate Candidates
	  */
	def createDuplicateCandidates(subjectPairs: RDD[(Subject, Subject, Double)]): RDD[DuplicateCandidates] = {
		val stagingTable = settings("stagingTable")
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
	def findDuplicates(blocks: RDD[Block]): RDD[(Subject, Subject, Double)] = {
		val threshold = settings("confidence").toDouble
		blocks
			.flatMap(_.crossProduct())
			.map(subjects => (subjects._1, subjects._2, compare(subjects._1, subjects._2)))
			.filter(_._3 >= threshold)
	}
}
