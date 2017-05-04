package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.deduplication.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
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
		val blockEvaluation = evaluateBlocks(blocks, s"BlockingJob_${System.currentTimeMillis()}")
		List(blockEvaluation).toAnyRDD()
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
	  * Uses input blocking scheme to generate blocks of subjects.
	  * @param subjects RDD to perform the blocking on.
	  * @param blockingSchemes blocking scheme to use to generate keys.
	  * @return subjects keyed by blocking keys and the blocking schemes tag
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
	  * Creates blocks based on the input blocking scheme
	  * @param subjects RDD to perform the blocking on
	  * @param stagingSubjects RDD to perform the blocking on
	  * @param blockingSchemes blocking schemes to use to generate keys.
	  * @return RDD of blocks.
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

	def mergeEvaluation(eval1: BlockEvaluation, eval2: BlockEvaluation): BlockEvaluation = {
		val blockingData = (eval1.data.toList ++ eval2.data.toList)
			.groupBy(_._1)
			.mapValues(_.flatMap(_._2))
		BlockEvaluation(eval1.id, blockingData, eval1.comment)
	}

	/**
	  * Executes methods on the blocked data to gain information.
	  *
	  * @param blocks Input data, partitioned in blocks.
	  * @param comment Note to add to the evaluated data.
	  * @return Blocks with size-value and comment.
	  */
	def evaluateBlocks(
		blocks: RDD[(String, Block)],
		comment: String
	): RDD[BlockEvaluation] = {
		val defaultKey = "key"
		val id = UUID.randomUUID()
		blocks.map { case (tag, block) =>
			val evalMap = Map(tag -> List(block.evaluate()))
			val evaluation = BlockEvaluation(id, evalMap, Option(comment))
			(defaultKey, evaluation)
		}.reduceByKey(mergeEvaluation)
		.values
	}
}
