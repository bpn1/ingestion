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

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.deduplication.blockingschemes._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.models.config.AttributeConfig
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Compares two groups of Subjects by first blocking with multiple Blocking Schemes, then compares all Subjects
  * in every block and filters all pairs below a given threshold.
  */
object Deduplication extends SparkJob {
	sparkOptions("spark.yarn.executor.memoryOverhead") = "8192"

	appName = "Deduplication"
	configFile = "deduplication.xml"
	val blockingSchemes = List[BlockingScheme](
		SimpleBlockingScheme("simple_scheme"),
		ListBlockingScheme("sectorBlocking_scheme", "gen_sectors")
	)

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = List(sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable")))
		val staging = List(sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable")))
		(subjects ++ staging).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Duplicates]()
			.head
			.saveToCassandra(settings("keyspaceDuplicatesTable"), settings("duplicatesTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Blocks the Subjects and finds duplicates between them between the Subjects and staged Subjects.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val List(subjects, staging) = input.fromAnyRDD[Subject]()
		val slaves = subjects.filter(_.isSlave)
		val blocks = Blocking.blocking(slaves, staging, blockingSchemes)
		val subjectPairs = findDuplicates(blocks.values, sc)
		val duplicates = createDuplicates(subjectPairs)
		List(duplicates).toAnyRDD()
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
		attributeConfigs: List[AttributeConfig] = Nil,
		scale: Int = 1
	): Double = {
		val scores = for {
			AttributeConfig(attribute, weight, configs) <- attributeConfigs
			subjectValues = subject1.get(attribute)
			stagingValues = subject2.get(attribute)
			if subjectValues.nonEmpty && stagingValues.nonEmpty
			config <- configs
		} yield CompareStrategy(attribute)(subjectValues, stagingValues, config) * weight
		scores.sum
	}

	/**
	  * Groups all found duplicates by the Subject whose duplicate they are and creates the corresponding
	  * DuplicateCandidates.
	  * @param subjectPairs RDD of Subject pairs containing all found duplicates
	  * @return RDD of grouped Duplicate Candidates
	  */
	def createDuplicates(subjectPairs: RDD[(Subject, Subject, Double)]): RDD[Duplicates] = {
		val stagingTable = settings("stagingTable")
		subjectPairs
			.map { case (subject, staging, score) =>
				((subject.id, subject.name), List(Candidate(staging.id, staging.name, score)))
			}.reduceByKey(_ ::: _)
			.map { case ((id, name), candidates) => Duplicates(id, name, stagingTable, candidates.distinct) }
	}

	/**
	  * Finds the duplicates of each block by comparing the Subjects and filtering all Subjects pairs below the
	  * threshold confidence.
	  * @param blocks RDD of BLocks containing the Subjects that are compared
	  * @return tuple of Subjects with their score, which is greater or equal the given threshold.
	  */
	def findDuplicates(blocks: RDD[Block], sc: SparkContext): RDD[(Subject, Subject, Double)] = {
		val threshold = settings("confidence").toDouble
		val confBroad = sc.broadcast(scoreConfigSettings)
		blocks
			.flatMap(_.crossProduct())
			.map { case (subject1, subject2) =>
				(subject1, subject2, compare(subject1, subject2, confBroad.value))
			}.filter(_._3 >= threshold)
	}
}
