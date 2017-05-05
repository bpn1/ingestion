package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.deduplication.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.deduplication.blockingschemes._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.TupleImplicits._

import scala.language.postfixOps
import scala.xml.XML
import scala.collection.mutable
import scala.io.Source

object BlockingJob extends SparkJob {
	appName = "Blocking"
	val configFile: Option[String] = None
	val settings = mutable.Map[String, String]()
	var blockingSchemes = List[BlockingScheme](new SimpleBlockingScheme)

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = List(sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable")))
		val staging = List(sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable")))
		(subjects ++ staging).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[BlockEvaluation]()
			.head
			.saveToCassandra(settings("keyspaceStatsTable"), settings("statsTable"))
	}
	// $COVERAGE-ON$

	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val inputSubjects = input.fromAnyRDD[Subject]()
		val subjects = inputSubjects.head
		val staging = inputSubjects(1)
		val geoCountryBlockingScheme = ListBlockingScheme("geo_city", "geo_country_scheme")
		val generalSectorBlockingScheme = ListBlockingScheme("gen_sectors", "gen_sectors_scheme")
		val simpleBlockingScheme = SimpleBlockingScheme("simple_scheme")
		val blockingSchemes = List(geoCountryBlockingScheme, generalSectorBlockingScheme, simpleBlockingScheme)
		val blocks = blocking(subjects, staging, blockingSchemes, false)
		val blockEvaluation = List(evaluateBlocks(blocks, s"BlockingJob_${System.currentTimeMillis()}"))
		blockEvaluation.toAnyRDD()
	}

	/**
	  * Reads the configuration from a xml file
	  * @return a list containing scoreConfig entities parsed from the xml file
	  */
	def parseConfig(): Unit = {
		val path = this.configFile.getOrElse("config.xml")
		val xml = XML.loadString(
			Source
				.fromURL(getClass.getResource(s"/$path"))
				.getLines()
				.mkString("\n")
		)

		val configSettings = xml \\ "config" \ "sourceSettings"
		for(node <- configSettings.head.child if node.text.trim.nonEmpty)
			settings(node.label) = node.text
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
			}
	}

	/**
	  * Counts the number of subjects per block and writes the statistical data to a Block Evaluation object.
	  * @param blocks blocked subjects
	  * @param comment note to add to the evaluated data
	  * @return Block Evaluation containing the block sizes per blocking scheme
	  */
	def evaluateBlocks(
		blocks: RDD[(String, Block)],
		comment: String
	): RDD[BlockEvaluation] = {
		blocks
			.map(_.map(identity, block => List(block.evaluate())))
			.reduceByKey(_ ++ _)
			.map { case (tag, blockList) =>
				BlockEvaluation(data = blockList, comment = Option(s"${tag}_$comment"))
			}
	}
}
