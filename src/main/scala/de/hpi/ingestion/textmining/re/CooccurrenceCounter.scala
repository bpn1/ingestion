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

package de.hpi.ingestion.textmining.re

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Cooccurrence, Sentence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Counts the cooccurrences in all sentences.
  */
object CooccurrenceCounter extends SparkJob {
	appName = "Cooccurrence Counter"
	cassandraSaveQueries += "TRUNCATE TABLE wikidumps.wikipediacooccurrences"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val sentences = sc.cassandraTable[Sentence](settings("keyspace"), settings("sentenceTable"))
		List(sentences).toAnyRDD()
	}

	/**
	  * Saves Sentences with entities to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Cooccurrence]()
			.head
			.saveToCassandra(settings("keyspace"), settings("cooccurrenceTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Transforms the entity links into entities and filters duplicate.
	  *
	  * @param sentence Sentence the entities will be extracted from
	  * @return List of unique entities in order of entity link offset
	  */
	def sentenceToEntityList(sentence: Sentence): List[String] = {
		sentence.entities.map(_.entity).distinct
	}

	/**
	  * Groups by entity list and counts cooccurrences.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val sentences = input.fromAnyRDD[Sentence]().head
		val entityLists = sentences
			.map(sentence => (sentenceToEntityList(sentence), 1))
			.filter(_._1.size > 1)
			.reduceByKey(_ + _)
			.map(Cooccurrence.tupled)
		List(entityLists).toAnyRDD()
	}
}
