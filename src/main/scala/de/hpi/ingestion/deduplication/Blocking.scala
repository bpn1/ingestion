package de.hpi.ingestion.deduplication

import java.util.UUID
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

/**
  * Blocks two groups of Subjects with multiple Blocking Schemes and evaluates the resulting blocks by counting their
  * sizes and writing the results to the Cassandra.
  */
object Blocking extends SparkJob {
	appName = "Blocking"
	configFile = "evaluation_deduplication.xml"
	var blockingSchemes = List[BlockingScheme](
		SimpleBlockingScheme("simple_scheme")
	)
	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects, staged Subjects and the GoldStandard from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
		val staging = sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable"))
		val goldStandard = sc.cassandraTable[(UUID, UUID)](
			settings("keyspaceGoldStandardTable"),
			settings("goldStandardTable")
		)
		List(subjects,staging).toAnyRDD() ::: List(goldStandard).toAnyRDD()
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
	  * Blocks the input Subjects and counts the block sizes.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val subjectInput = input.fromAnyRDD[Subject]()
		val subjects = subjectInput.head
		val staging = subjectInput(1)
		val goldStandard = input.fromAnyRDD[(UUID, UUID)]().last
		val comment = args.headOption.getOrElse("Blocking")
		val goldenBroadcast = sc.broadcast[Set[(UUID, UUID)]](goldStandard.collect.toSet)
		val filterUndefined = settings(false).getOrElse("filterUndefined", "false") == "true"
		val blockEvaluation = List(evaluationBlocking(
			subjects,
			staging,
			goldenBroadcast.value,
			this.blockingSchemes,
			filterUndefined,
			comment
		))
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
		goldStandard: Set[(UUID, UUID)],
		blockingSchemes: List[BlockingScheme],
		filterUndefined: Boolean = true,
		comment: String
	): RDD[BlockEvaluation] = {
		val subjectBlocks = blocking(subjects, blockingSchemes).map(_.map(identity, subject => subject.id))
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes).map(_.map(identity, subject => subject.id))
		val jobid = UUIDs.timeBased()
		val blocks = subjectBlocks.cogroup(stagingBlocks).filter { case ((key, tag), (subjectList, stagingList)) =>
			!filterUndefined || !blockingSchemes.find(_.tag == tag).map(_.undefinedValue).contains(key)
		}
		val duplicates = blocks.map { case ((key, tag), (subjectList, stagingList)) =>
			val cross = subjectList.cross(stagingList)
			val duplicates = cross.filter(goldStandard)
			val precision = if (cross.nonEmpty) duplicates.size.toDouble / cross.size.toDouble else 0.0
			val blockStats = Set(BlockStats(key, subjectList.size, stagingList.size, precision))
			(duplicates, (tag, blockStats))
		}
		val accuracy = duplicates
			.map { case (duplicates, (tag, block)) =>
				(tag, duplicates)
			}.distinct
			.map { case (tag, duplicates) => (tag, duplicates.size) }
			.reduceByKey(_ + _)
			.collect
			.toMap
			.mapValues(_.toDouble / goldStandard.size.toDouble)
			.map(identity)

		duplicates
			.values
			.reduceByKey(_ ++ _)
			.map { case (tag, blockStats) =>
				val tagAccuracy = accuracy.getOrElse(tag, 0.0)
				val blockComment = Option(s"$comment; accuracy: $tagAccuracy")
				BlockEvaluation(jobid, tag, blockStats, blockComment)
			}
	}
}
