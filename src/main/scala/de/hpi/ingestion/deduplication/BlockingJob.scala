package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models._

import de.hpi.ingestion.deduplication.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.xml.XML
import scala.collection.mutable
import scala.io.Source

object BlockingJob extends SparkJob {
	appName = "Blocking"
	val configFile: Option[String] = None
	val settings = mutable.Map[String, String]()
	var blockingSchemes = List[BlockingScheme](new SimpleBlockingScheme)

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = List(sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable")))
		val staging = List(sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable")))
		(subjects ++ staging).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Blocking]()
			.head
			.saveToCassandra(settings("keyspaceStatsTable"), settings("statsTable"))
	}

	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val inputSubjects = input.fromAnyRDD[Subject]()
		val subjects = inputSubjects.head
		val staging = inputSubjects(1)
		val blockingSchemes = List(new SimpleBlockingScheme)
		val blocks = blocking(subjects, staging, blockingSchemes)
		val blockEvaluation = evaluateBlocks(blocks, blockingSchemes)
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
	  * @param blockingSchemes blocking schemes to use to generate keys.
	  * @return subjects grouped by blocking keys
	  */
	def blocking(
		subjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme]
	): List[RDD[(String, List[Subject])]] = {
		blockingSchemes.map { scheme =>
			subjects.flatMap { subject =>
				val blockingKeys = scheme.generateKey(subject).distinct
				blockingKeys.map((_, List(subject)))
			}.reduceByKey(_ ::: _)
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
		blockingSchemes: List[BlockingScheme]
	): RDD[Blocking] = {
		val subjectBlocks = blocking(subjects, blockingSchemes)
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes)

		(subjectBlocks, stagingBlocks, blockingSchemes).zipped.map { case (subjectBlock, stagingBlock, block) =>
			subjectBlock
			.join(stagingBlock)
			.map { case (key, (subjectList, stagingList)) =>
				val comment = s"${appName}_${block.tag}_${System.currentTimeMillis()}"
				Blocking(key = Option(key), subjects = subjectList, staging = stagingList, comment = Option(comment))
			}
		}.reduce(_.union(_))
	}
}
