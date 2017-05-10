package de.hpi.ingestion.deduplication

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.deduplication.blockingschemes._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.TupleImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.xml.{Node, XML}

/**
  * Blocks two groups of Subjects with multiple Blocking Schemes and evaluates the resulting blocks by counting their
  * sizes and writing the results to the Cassandra.
  */
object Blocking extends SparkJob {
	appName = "Blocking"
	val configFile: Option[String] = None
	var settings = Map[String, String]()
	var blockingSchemes = List[BlockingScheme](
		ListBlockingScheme("geo_country_scheme", "geo_country"),
		ListBlockingScheme("geo_coords_scheme", "geo_coords"),
		SimpleBlockingScheme("simple_scheme"))

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects and staged Subjects from the Cassandra.
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
	  * Saves the Blocking Evaluation to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[BlockEvaluation]()
			.head
			.saveToCassandra(settings("keyspaceStatsTable"), settings("statsTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Parses the config before asserting the Conditions of the super class.
	  * @param args arguments of the program
	  * @return true if the program can continue, false if it should be terminated
	  */
	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	/**
	  * Blocks the input Subjects and counts the block sizes.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val List(subjects, staging) = input.fromAnyRDD[Subject]()
		val comment = args.headOption.getOrElse("Blocking")
		val blockEvaluation = List(evaluationBlocking(subjects, staging, blockingSchemes, false, comment))
		blockEvaluation.toAnyRDD()
	}

	/**
	  * Sets the used blocking schemes.
	  * @param schemes List of Blocking Schemes used in the blocking process
	  */
	def setBlockingSchemes(schemes: BlockingScheme*): Unit = setBlockingSchemes(schemes.toList)

	/**
	  * Sets the used blocking schemes.
	  * @param schemes List of Blocking Schemes used in the blocking process
	  */
	def setBlockingSchemes(schemes: List[BlockingScheme]): Unit = {
		blockingSchemes = if(schemes.nonEmpty) schemes else List(new SimpleBlockingScheme)
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
	}

	/**
	  * Uses input blocking schemes to generate blocks of subjects. Every subject generates an entry for every key
	  * element for every blocking scheme.
	  * @param subjects RDD to perform the blocking on
	  * @param blockingSchemes List of blocking schemes used to generate keys
	  * @return subjects keyed by a blocking key and the blocking schemes tag
	  */
	def blocking(
		subjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme]
	): RDD[((String, String), Subject)] = {
		subjects
			.flatMap { subject =>
				blockingSchemes.flatMap { scheme =>
					val blockingKeys = scheme.generateKey(subject).distinct
					blockingKeys.map(key => ((key, scheme.tag), subject))
				}
			}
	}

	/**
	  * Blocks the input Subjects using the provided blocking schemes.
	  * @param subjects RDD to perform the blocking on
	  * @param stagingSubjects RDD to perform the blocking on
	  * @param blockingSchemes blocking schemes used to generate keys
	  * @return RDD of blocks keyed by the tag of the blocking scheme used to genrate the blocking
	  */
	def blocking(
		subjects: RDD[Subject],
		stagingSubjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme],
		filterUndefined: Boolean = true
	): RDD[(String, Block)] = {
		val subjectBlocks = blocking(subjects, blockingSchemes)
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes)
		subjectBlocks
			.cogroup(stagingBlocks)
			.map { case ((key, tag), (subjectList, stagingList)) =>
				(tag, Block(key = key, subjects = subjectList.toList, staging = stagingList.toList))
			}.filter { case (tag, block) =>
				!(blockingSchemes.find(_.tag == tag).map(_.undefinedValue).contains(block.key) && filterUndefined)
			}
	}

	/**
	  * Blocks the input Subjects using the provided blocking schemes and evaluates the blocks. This method reduces
	  * the amount of data shuffled across the network for better performance.
	  * @param subjects RDD containing the Subjects
	  * @param stagingSubjects RDD containing the staged Subjects
	  * @param blockingSchemes blocking schemes used to generate keys
	  * @param filterUndefined whether or not the undefined blocks will be filtered (defaults to true)
	  * @param comment additional comment added to the data entries
	  * @return RDD of Block Evaluations containing the Block sizes
	  */
	def evaluationBlocking(
		subjects: RDD[Subject],
		stagingSubjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme],
		filterUndefined: Boolean = true,
		comment: String
	): RDD[BlockEvaluation] = {
		val subjectBlocks = blocking(subjects, blockingSchemes).map(_.map(identity, s => 1))
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes).map(_.map(identity, s => 1))
		val jobid = UUIDs.timeBased()

		subjectBlocks
			.cogroup(stagingBlocks)
			.filter { case ((key, tag), (subjectList, stagingList)) =>
				val isUndefined = blockingSchemes
					.find(_.tag == tag)
					.map(_.undefinedValue)
					.contains(key)
				!(isUndefined && filterUndefined)
			}.map { case ((key, tag), (subjectList, stagingList)) =>
				val blockData = BlockStats(key, subjectList.sum, stagingList.sum)
				(tag, Set(blockData))
			}.reduceByKey(_ ++ _)
			.map { case (tag, blockStatSet) =>
				BlockEvaluation(jobid, tag, blockStatSet, Option(comment))
			}
	}
}
