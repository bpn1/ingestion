/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	  * Counts the number of blocks for each blocking scheme and in total.
	  * @param blocks blocked subjects
	  * @param sumTag tag to be set for the total block count
	  * @return blocktags (information on the blocking scheme) with the associated blockcounts
	  */
	def calculateBlockCount(
		blocks: RDD[((String, String), (Iterable[UUID], Iterable[UUID]))],
		sumTag: String
	): Map[String, Int] = {
		val blockCount = blocks
			.map{ case ((key, tag), subjects) => (tag, 1) }
			.reduceByKey(_ + _)
			.collect
			.toMap
		val totalBlockCount = blockCount
			.values
			.sum

		blockCount + (sumTag -> totalBlockCount)
	}

	/**
	  * Counts the number of comparisons for each blocking scheme and in total.
	  * @param blocks blocked subjects
	  * @param sumTag tag to be set for the total block count
	  * @return blocktags (information on the blocking scheme) with the associated comparison count
	  */
	def calculateCompareCount(
		blocks: RDD[((String, String), (Iterable[UUID], Iterable[UUID]))],
		sumTag: String
	): Map[String, BigInt] = {
		val blockComparisons = blocks
			.map { case ((key, tag), (subject, staging)) => tag -> (BigInt(subject.size) * BigInt(staging.size)) }
			.reduceByKey(_ + _)
			.collect
			.toMap
		val sumComparisons = blocks
			.flatMap { case ((key, tag), (subject, staging)) => subject.cross(staging) }
			.distinct
			.count

		blockComparisons + (sumTag -> BigInt(sumComparisons))
	}

	/**
	  * Calculates the relative amount of duplicates we could find in our comparisons for each blocking scheme.
	  * Uses the goldstandard to know, how many duplicates we should be able to find.
	  * @param duplicates duplicates from our input that fit the goldstandard
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @param sumTag tag to be set for the total block count
	  * @return a double for each blocking scheme-tag, representative for the relative amount of duplicates
	  */
	def calculatePairsCompleteness(
		duplicates: RDD[(Traversable[(UUID, UUID)], (String, Set[BlockStats]))],
		goldStandard: Set[(UUID, UUID)],
		sumTag: String
	): Map[String, Double] = {
		val goldStandardSize = goldStandard.size.toDouble
		val pairsCompleteness = duplicates
			.flatMap { case (duplicateIDs, (tag, block)) => duplicateIDs.map((tag, _)) }
			.distinct
			.map { case (tag, duplicate) => (tag, 1) }
			.reduceByKey(_ + _)
			.mapValues(_.toDouble / goldStandardSize)
			.collect
			.toMap
		val sumPairsCompleteness = duplicates
			.flatMap { case (duplicateIDs, (tag, block)) => duplicateIDs }
			.distinct
			.count()
			.toDouble / goldStandard.size.toDouble

		pairsCompleteness + (sumTag -> sumPairsCompleteness)
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
			.filter { case ((key, tag), (subject, staging)) =>
				val isUndefined = blockingSchemes.find(_.tag == tag).map(_.undefinedValue).contains(key)
				val isHuge = (subject.size * staging.size) > maxBlockSize
				!(isHuge || filterUndefined && isUndefined)
			}
		val duplicates = blocks.map(createDuplicateStats(_, goldStandard))
		val sumTag = s"sum ${blockingSchemes.map(_.tag).mkString(", ")}"
		val pairsCompleteness = calculatePairsCompleteness(duplicates, goldStandard, sumTag)
		val blockCount = calculateBlockCount(blocks, sumTag)
		val comparisonCount = calculateCompareCount(blocks, sumTag)

		val statsSum = duplicates
			.values
			.map { case (tag, blockStats) => (sumTag, blockStats) }
			.reduceByKey(_ ++ _)
		duplicates
			.values
			.reduceByKey(_ ++ _)
			.union(statsSum)
			.flatMap { case (tag, blockStats) =>
				val minBlockSize = settings("minBlockSize").toInt
				val filteredStats = blockStats.filter(!filterSmall || !_.isTiny(minBlockSize))
				val blockComment = Option(s"$comment")
				val tagCompleteness = pairsCompleteness.getOrElse(tag, 0.0)
				val tagBlockCount = blockCount.getOrElse(tag, 0)
				comparisonCount
					.get(tag)
					.map(BlockEvaluation(jobid, tag, filteredStats, blockComment, tagCompleteness, tagBlockCount, _))
			}
	}
}
