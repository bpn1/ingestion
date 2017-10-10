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
import de.hpi.ingestion.textmining.models.{Cooccurrence, Sentence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Counts the cooccurrences in all sentences.
  */
class CooccurrenceCounter extends SparkJob {
	import CooccurrenceCounter._
	appName = "Cooccurrence Counter"
	cassandraSaveQueries += "TRUNCATE TABLE wikidumps.wikipediacooccurrences"
	configFile = "textmining.xml"

	var sentences: RDD[Sentence] = _
	var cooccurrences: RDD[Cooccurrence] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		sentences = sc.cassandraTable[Sentence](settings("keyspace"), settings("sentenceTable"))
	}

	/**
	  * Saves Sentences with entities to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		cooccurrences.saveToCassandra(settings("keyspace"), settings("cooccurrenceTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Groups by entity list and counts cooccurrences.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		cooccurrences = sentences
			.map(sentence => (sentenceToEntityList(sentence), 1))
			.filter(_._1.size > 1)
			.reduceByKey(_ + _)
			.map(Cooccurrence.tupled)
	}
}

object CooccurrenceCounter {
	/**
	  * Transforms the entity links into entities and filters duplicate.
	  *
	  * @param sentence Sentence the entities will be extracted from
	  * @return List of unique entities in order of entity link offset
	  */
	def sentenceToEntityList(sentence: Sentence): List[String] = {
		sentence.entities.map(_.entity).distinct
	}
}
