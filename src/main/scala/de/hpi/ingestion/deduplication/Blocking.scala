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
		SimpleBlockingScheme("simple_scheme"),
		LastLettersBlockingScheme("lastLetters_scheme")
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
		val filterSmall = settings(false).getOrElse("filterSmall", "false") == "true"
		val blockEvaluation = List(evaluationBlocking(
			subjects,
			staging,
			goldenBroadcast.value,
			this.blockingSchemes,
			filterUndefined,
			filterSmall,
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
	  * Calculates the relative amount of duplicates we could find in our comparisonsfor all blocking schemes.
	  * Uses the goldstandard to know, how many duplicates we should be able to find.
	  * @param duplicates duplicates from our input that fit the goldstandard
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @return a double, representative for the relative amount of total duplicates
	  */
	def calculateTotalPairwiseCompleteness(
		duplicates: RDD[(Traversable[(UUID, UUID)], (String, Set[BlockStats]))],
		goldStandard: Set[(UUID, UUID)]
	): Double = {
		duplicates
			.flatMap { case (duplicateIDs, (tag, block)) => duplicateIDs }
			.distinct
			.count()
			.toDouble / goldStandard.size.toDouble
	}

	/**
	  * Calculates the relative amount of duplicates we could find in our comparisons for each blocking scheme.
	  * Uses the goldstandard to know, how many duplicates we should be able to find.
	  * @param duplicates duplicates from our input that fit the goldstandard
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @return a double for each blocking scheme-tag, representative for the relative amount of duplicates
	  */
	def calculatePairwiseCompleteness(
		duplicates: RDD[(Traversable[(UUID, UUID)], (String, Set[BlockStats]))],
		goldStandard: Set[(UUID, UUID)]
	): Map[String, Double] = {
		duplicates
			.flatMap { case (duplicateIDs, (tag, block)) =>
			duplicateIDs.map((tag, _))
			}.distinct
			.map { case (tag, duplicate) => (tag, 1) }
			.reduceByKey(_ + _)
			.collect
			.toMap
			.mapValues(_.toDouble / goldStandard.size.toDouble)
			.map(identity)
	}

	/**
	  * Uses the goldstandard to find all relevant duplicate-pairs to be possibly found in deduplication.
	  * @param block blocked subjects
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @return duplicates from the goldstandard found in the input block
	  */
	def createDuplicateStats(
		block: ((String, String), (Traversable[UUID], Traversable[UUID])),
		goldStandard: Set[(UUID, UUID)]
	): (Traversable[(UUID, UUID)], (String, Set[BlockStats])) = {
		val ((key, tag), (subjectList, stagingList)) = block
		val cross = subjectList.cross(stagingList)
		val duplicates = cross.filter(goldStandard)
		val precision = if (cross.nonEmpty) duplicates.size.toDouble / cross.size.toDouble else 0.0
		val blockStats = Set(BlockStats(key, subjectList.size, stagingList.size, precision))
		(duplicates, (tag, blockStats))
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
		val maxBlockSize = settings("maxBlockSize").toInt
		subjectBlocks
			.cogroup(stagingBlocks)
			.map { case ((key, tag), (subjectList, stagingList)) =>
				(tag, Block(key = key, subjects = subjectList.toList, staging = stagingList.toList))
			}.filter { case (tag, block) =>
				!(blockingSchemes.find(_.tag == tag).map(_.undefinedValue).contains(block.key) && filterUndefined)
			}.filter { case (tag, block) =>
				(block.staging.size * block.subjects.size) < maxBlockSize // filter huge blocks
			}
	}
	// scalastyle:off method.length
	/**
	  * Blocks the input Subjects using the provided blocking schemes and evaluates the blocks. This method reduces
	  * the amount of data shuffled across the network for better performance.
	  * @param subjects RDD containing the Subjects
	  * @param stagingSubjects RDD containing the staged Subjects
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @param blockingSchemes blocking schemes used to generate keys
	  * @param filterUndefined whether or not the undefined blocks will be filtered (defaults to true)
	  * @param filterSmall whether or not very small blocks will be filtered (defaults to true)
	  * @param comment additional comment added to the data entries
	  * @return RDD of Block Evaluations containing the Block sizes
	  */
	def evaluationBlocking(
		subjects: RDD[Subject],
		stagingSubjects: RDD[Subject],
		goldStandard: Set[(UUID, UUID)],
		blockingSchemes: List[BlockingScheme],
		filterUndefined: Boolean = true,
		filterSmall: Boolean = true,
		comment: String
	): RDD[BlockEvaluation] = {
		val subjectBlocks = blocking(subjects, blockingSchemes).map(_.map(identity, subject => subject.id))
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes).map(_.map(identity, subject => subject.id))
		val maxBlockSize = settings("maxBlockSize").toInt
		val jobid = UUIDs.timeBased()
		val blocks = subjectBlocks
			.cogroup(stagingBlocks)
			.filter { case ((key, tag), (subjectList, stagingList)) =>
				!filterUndefined || !blockingSchemes.find(_.tag == tag).map(_.undefinedValue).contains(key)
		}.filter { case ((key, tag), (subject, staging)) => (subject.size * staging.size) < maxBlockSize }

		val blockComparisons = blocks
			.map { case ((key, tag), (subject, staging)) =>
				(tag, subject.size.toLong * staging.size.toLong) }.reduceByKey(_ + _).collect.toMap
		val totalComparisons = blockComparisons.map{ case (_, comparisons) => comparisons }.sum
		val blockCount = blocks.map{ case ((key, tag), subjects) => (tag, 1) }.reduceByKey(_ + _).collect.toMap
		val totalBlockCount = blockCount.values.sum
		val duplicates = blocks.map(createDuplicateStats(_, goldStandard))

		var pairwiseCompleteness = calculatePairwiseCompleteness(duplicates, goldStandard)
		val totalPairwiseCompleteness = calculateTotalPairwiseCompleteness(duplicates, goldStandard)
		val sumTag = s"sum ${blockingSchemes.map(_.tag).mkString(", ")}"
		pairwiseCompleteness ++= Map(sumTag -> totalPairwiseCompleteness)
		val allComparisons = blockComparisons ++ Map(sumTag -> totalComparisons)
		val allBlockCounts = blockCount ++ Map(sumTag -> totalBlockCount)
		// ToDo: totalPairwiseCompleteness in PairwiseCompleteness berechnen und dort anhängen (comparisons auch)
		val statsSum = duplicates
			.values
			.map { case (tag, blockStats) => (sumTag, blockStats) }
			.reduceByKey(_ ++ _)
		duplicates
			.values
			.reduceByKey(_ ++ _)
			.union(statsSum)
			.map { case (tag, blockStats) =>
				val minBlockSize = settings("minBlockSize").toInt
				val filteredStats = blockStats.filter(stat =>
					!filterSmall || (stat.numsubjects * stat.numstaging) > minBlockSize) // don't write tiny blocks
				val tagCompleteness = pairwiseCompleteness.getOrElse(tag, 0.0)
				val tagComparisons: Long = allComparisons.getOrElse(tag, 0)
				val tagBlockCount = allBlockCounts.getOrElse(tag, 0)
				val blockComment = Option(s"$comment")
				BlockEvaluation(jobid, tag, filteredStats, blockComment, tagCompleteness, tagBlockCount, tagComparisons)
			}
	}
}
