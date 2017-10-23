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
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * Blocks two groups of Subjects with multiple Blocking Schemes and evaluates the resulting blocks by counting their
  * sizes and writing the results to the Cassandra.
  */
class Blocking extends SparkJob {
	import Blocking._
	appName = "Blocking"
	configFile = "blocking.xml"

	var blockingSchemes: List[BlockingScheme] = List(SimpleBlockingScheme("simple_scheme"))
	var subjectReductionFunction: (Subject => Subject) = identity[Subject]
	var filterUndefined: Boolean = true
	var filterSmall: Boolean = true
	var minBlockSize: Long = 1
	var maxBlockSize: Long = Long.MaxValue
	var calculatePC: Boolean = false

	var subjects: RDD[Subject] = _
	var stagedSubjects: RDD[Subject] = _
	var goldStandard: RDD[(UUID, UUID)] = _
	var blockEvaluation: RDD[BlockEvaluation] = _

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects, staged Subjects and the GoldStandard from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
		stagedSubjects = sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable"))
		goldStandard = sc.cassandraTable[(UUID, UUID)](
			settings("keyspaceGoldStandardTable"),
			settings("goldStandardTable"))
	}
	/**
	  * Saves the Blocking Evaluation to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		blockEvaluation.saveToCassandra(settings("keyspaceStatsTable"), settings("statsTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Blocks the input Subjects and counts the block sizes.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val comment = conf.commentOpt.getOrElse("Blocking")
		val goldenBroadcast = sc.broadcast[Set[(UUID, UUID)]](goldStandard.collect.toSet)
		settings.get("filterUndefined").foreach(setFilterUndefined)
		settings.get("filterSmall").foreach(setFilterSmall)
		settings.get("minBlockSize").foreach(setMinBlockSize)
		settings.get("maxBlockSize").foreach(setMaxBlockSize)
		evaluationBlocking(goldenBroadcast.value, comment)
	}

	/**
	  * Blocks the input Subjects using the provided blocking schemes and evaluates the blocks. This method reduces
	  * the amount of data shuffled across the network for better performance.
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @param comment additional comment added to the data entries
	  */
	def evaluationBlocking(goldStandard: Set[(UUID, UUID)], comment: String): Unit = {
		val jobid = UUIDs.timeBased()
		subjectReductionFunction = (subject: Subject) =>
			Subject(id = subject.id, master = subject.master, datasource = subject.datasource)
		val blocks = blocking().cache()
		val duplicates = blocks.map(createDuplicateStats(_, goldStandard))
		val combinedSchemesTag = s"combined ${blockingSchemes.map(_.tag).mkString(", ")}"
		var pairsCompleteness = Map.empty[String, Double]
		if(calculatePC) {
			pairsCompleteness = calculatePairsCompleteness(duplicates, goldStandard, combinedSchemesTag)
		}
		val blockCount = calculateBlockCount(blocks, combinedSchemesTag)
		val comparisonCount = calculateComparisonCount(blocks, combinedSchemesTag)
		blockEvaluation = duplicates
			.values
			.flatMap { case (tag, blockStats) =>
				val blockingSchemeTuple = (tag, Set(blockStats))
				val combinedSchemesTuple = (combinedSchemesTag, Set(blockStats))
				List(blockingSchemeTuple, combinedSchemesTuple)
			}.reduceByKey(_ ++ _)
			.map { case (tag, blockStatsSet) =>
				val filteredBlockStatsSet = blockStatsSet.filter { blockStats =>
					val isTooSmall = blockStats.numComparisons < minBlockSize
					!(filterSmall && isTooSmall)
				}
				BlockEvaluation(
					jobid,
					tag,
					filteredBlockStatsSet,
					Option(comment),
					pairsCompleteness.getOrElse(tag, 0.0),
					blockCount.getOrElse(tag, 0),
					BigInt(comparisonCount.getOrElse(tag, 0L)))
			}
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
	  * Sets maximum size of Blocks.
	  * @param maxSize maximum size of a Block
	  * @tparam T type of the maxSize parameter. Can either be a String, Int or Long.
	  */
	def setMaxBlockSize[T](maxSize: T): Unit = {
		maxSize match {
			case size: Long => maxBlockSize = size
			case size: Int => maxBlockSize = size.toLong
			case size: String => maxBlockSize = size.toLong
			case _ =>
		}
	}

	/**
	  * Sets minimum size of Blocks.
	  * @param minSize minimum size of a Block
	  * @tparam T type of the minSize parameter. Can either be a String, Int or Long.
	  */
	def setMinBlockSize[T](minSize: T): Unit = {
		minSize match {
			case size: Long => minBlockSize = size
			case size: Int => minBlockSize = size.toLong
			case size: String => minBlockSize = size.toLong
			case _ =>
		}
	}

	/**
	  * Sets the filter undefined Blocks flag.
	  * @param flag the Boolean value as String
	  */
	def setFilterUndefined(flag: String): Unit = {
		filterUndefined = Try(flag.toBoolean).getOrElse(false)
	}

	/**
	  * Sets the filter small Blocks flag.
	  * @param flag the Boolean value as String
	  */
	def setFilterSmall(flag: String): Unit = {
		filterSmall = Try(flag.toBoolean).getOrElse(false)
	}

	/**
	  * If a Simple Blocking Schemes was used Blocks with a shorter key are merged with Blocks which have a normal key.
	  * The shorter Blocks are added to every normal Block whose key has the key of the shorter Block as prefix.
	  * @param blocks Blocks created by the Blocking Schemes
	  * @return the merged Blocks containing their own Subjects and the Subjects of the Blocks with a smaller key but
	  *         the same prefix as well as every unaffected Block
	  */
	def mergeSubsetBlocks(blocks: RDD[(String, Block)]): RDD[(String, Block)] = {
		val simpleSchemeOpt = blockingSchemes.find(_.isInstanceOf[SimpleBlockingScheme])
		if(simpleSchemeOpt.isEmpty) {
			return blocks
		}
		val simpleScheme = simpleSchemeOpt.get.asInstanceOf[SimpleBlockingScheme]
		val nonSimpleBlocks = blocks.filter(_._1 != simpleScheme.tag)
		val simpleBlocks = blocks
			.filter(_._1 == simpleScheme.tag)
			.values
		val shorterBlocks = simpleBlocks.filter(_.key.length < simpleScheme.length)
		val longerBlocks = simpleBlocks.filter(_.key.length == simpleScheme.length)
		if(shorterBlocks.isEmpty()) {
			return blocks
		}
		val minKeyLength = shorterBlocks
			.map(_.key.length)
			.min
		val valueSubsets = (minKeyLength until simpleScheme.length).toList
		val longerMatches = longerBlocks.flatMap { block =>
			val subKeys = valueSubsets.map(length => block.key.slice(0, length))
			subKeys.map((_, block.key))
		}
		val shortBlockMap = shorterBlocks
			.map(block => (block.key, block))
			.cogroup(longerMatches)
			.values
			.filter(_._1.nonEmpty)
			.flatMap { case (shortBlocks, longBlockKeys) =>
				longBlockKeys.map((_, shortBlocks.toList))
			}.reduceByKey(_ ++ _)
			.collect
			.toMap
		val mergedBlocks = longerBlocks.map { block =>
			val prefixBlocks = shortBlockMap.getOrElse(block.key, Nil)
			block.copy(
				subjects = block.subjects ++ prefixBlocks.flatMap(_.subjects),
				staging = block.staging ++ prefixBlocks.flatMap(_.staging)
			)
		}
		mergedBlocks
			.union(shorterBlocks)
			.map((simpleScheme.tag, _))
			.union(nonSimpleBlocks)
	}

	/**
	  * Blocks the input Subjects using the provided blocking schemes.
	  * @return RDD of blocks keyed by the tag of the blocking scheme used to genrate the blocking
	  */
	def blocking(): RDD[(String, Block)] = {
		val subjectBlocks = blockSubjects(subjects, blockingSchemes, subjectReductionFunction)
		val stagingBlocks = blockSubjects(stagedSubjects, blockingSchemes, subjectReductionFunction)
		val undefinedMap = blockingSchemes
			.map(scheme => (scheme.tag, scheme.undefinedValue))
			.toMap
		val blocks = subjectBlocks
			.cogroup(stagingBlocks)
			.map { case ((key, tag), (subjectList, stagingList)) =>
				(tag, Block(key = key, subjects = subjectList.toList, staging = stagingList.toList))
			}
		val mergedBlocks = mergeSubsetBlocks(blocks)
		mergedBlocks
			.filter { case (tag, block) =>
				val isUndefined = undefinedMap(tag) == block.key && filterUndefined
				val numComparisons = block.numComparisons
				val isTooSmall = numComparisons < minBlockSize && filterSmall
				!(isUndefined || isTooSmall)
			}.flatMap { case (tag, largeBlock) =>
				val splitBlocks = Block.split(largeBlock, maxBlockSize.toInt)
				val blocks = splitBlocks.length match {
					case 1 => splitBlocks
					case _ => splitBlocks.zipWithIndex.map { case (block, i) => block.copy(key = s"${block.key}_$i") }
				}
				blocks.map((tag, _))
			}.partitionBy(new HashPartitioner(7 * 32))
	}
}
object Blocking {
	/**
	  * Uses input blocking schemes to generate blocks of subjects. Every subject generates an entry for every key
	  * element for every blocking scheme.
	  * @param subjects RDD to perform the blocking on
	  * @param blockingSchemes List of blocking schemes used to generate keys
	  * @param f function that is applied to each Subject after the blocking to, e.g., reduce the data for evaluation
	  * @return subjects keyed by a blocking key and the blocking schemes tag
	  */
	def blockSubjects(
		subjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme],
		f: (Subject => Subject) = identity[Subject]
	): RDD[((String, String), Subject)] = {
		subjects
			.flatMap { subject =>
				blockingSchemes.flatMap { scheme =>
					val blockingKeys = scheme.generateKey(subject).distinct
					blockingKeys.map(key => ((key, scheme.tag), f(subject)))
				}
			}
	}

	/**
	  * Counts the number of blocks for each blocking scheme and in total.
	  * @param blocks tuple of tag of the Blocking Scheme and the generated Block
	  * @param combinedSchemesTag tag to be set for the combined number of blocks
	  * @return Blocking Scheme tags with the associated number of blocks
	  */
	def calculateBlockCount(
		blocks: RDD[(String, Block)],
		combinedSchemesTag: String
	): Map[String, Int] = {
		val schemeComparisons = blocks
			.map { case (tag, block) => (tag, 1) }
			.reduceByKey(_ + _)
			.collect
			.toMap
		val totalBlockCount = schemeComparisons.values.sum
		schemeComparisons + (combinedSchemesTag -> totalBlockCount)
	}

	/**
	  * Counts the number of comparisons for each blocking scheme and in total.
	  * @param blocks tuple of tag of the Blocking Scheme and the generated Block
	  * @param combinedSchemesTag tag to be set for the combined number of blocks
	  * @return blocktags (information on the blocking scheme) with the associated comparison count
	  */
	def calculateComparisonCount(
		blocks: RDD[(String, Block)],
		combinedSchemesTag: String
	): Map[String, Long] = {
		val blockComparisons = blocks
			.map { case (tag, block) => (tag, block.numComparisons) }
			.reduceByKey(_ + _)
			.collect
			.toMap
		val combinedComparisons = blockComparisons.values.sum
		blockComparisons + (combinedSchemesTag -> combinedComparisons)
	}

	/**
	  * Calculates the relative amount of duplicates we could find in our comparisons for each blocking scheme.
	  * Uses the goldstandard to know, how many duplicates we should be able to find.
	  * @param duplicates duplicates from our input that fit the goldstandard
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @param combinedSchemesTag tag to be set for the total block count
	  * @return a double for each blocking scheme-tag, representative for the relative amount of duplicates
	  */
	def calculatePairsCompleteness(
		duplicates: RDD[(List[(UUID, UUID)], (String, BlockStats))],
		goldStandard: Set[(UUID, UUID)],
		combinedSchemesTag: String
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

		pairsCompleteness + (combinedSchemesTag -> sumPairsCompleteness)
	}

	/**
	  * Uses the goldstandard to find all relevant duplicate-pairs to be possibly found in deduplication.
	  * @param taggedBlock tuple of tag of the Blocking Scheme and the generated Block
	  * @param goldStandard tuples of duplicateIDs we want to find
	  * @return duplicates from the goldstandard found in the input block
	  */
	def createDuplicateStats(
		taggedBlock: (String, Block),
		goldStandard: Set[(UUID, UUID)]
	): (List[(UUID, UUID)], (String, BlockStats)) = {
		val (tag, block) = taggedBlock
		val duplicates = block
			.crossProduct()
			.map { case (subject1, subject2) => (subject1.id, subject2.id) }
			.filter(goldStandard)
		var precision = duplicates.size.toDouble / block.numComparisons
		if(precision.isNaN) precision = 0.0
		val blockStats = BlockStats(block.key, block.subjects.length, block.staging.length, precision)
		(duplicates, (tag, blockStats))
	}
}
