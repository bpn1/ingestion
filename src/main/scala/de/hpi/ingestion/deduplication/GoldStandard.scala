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
import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD

class GoldStandard extends SparkJob {
	import GoldStandard._
	appName = "GoldStandard_v1.0"
	configFile = "goldstandard.xml"

	var dbpediaSubjects: RDD[Subject] = _
	var wikidataSubjects: RDD[Subject] = _
	var goldStandard: RDD[(UUID, UUID)] = _

	// $COVERAGE-OFF$
	/**
	  * Loads DBpedia and Wikidata from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		dbpediaSubjects = sc.cassandraTable[Subject](settings("keyspaceDBpediaTable"), settings("dBpediaTable"))
		wikidataSubjects = sc.cassandraTable[Subject](settings("keyspaceWikidataTable"), settings("wikiDataTable"))
	}

	/**
	  * Saves joined DBpedia and Wikidata to output table in keyspace keyspace.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		goldStandard.saveToCassandra(settings("keyspaceGoldStandardTable"), settings("goldStandardTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Joins DBpedia and Wikidata.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		goldStandard = join(dbpediaSubjects, wikidataSubjects)
	}
}

object GoldStandard {
	/**
	  * Keys Subjects by a property value
	  * @param rdd Subjects
	  * @param property Property to key by
	  * @return RDD containing Subjects keyed by a value
	  */
	def keyBySingleProperty(rdd: RDD[Subject], property: String): RDD[(String, Subject)] = {
		rdd
			.filter(_.properties.contains(property))
			.map { subject =>
				val keyOpt = subject.properties(property).headOption
				(keyOpt.getOrElse(""), subject)
			}
	}

	/**
	  * Join DBpedia and Wikidata together on a property.
	  * @param property The property to be joined on.
	  * @param dbpedia Subjects from DBpedia
	  * @param wikidata Subjects from Wikidata
	  * @return RDD containing pairs of UUIDs
	  */
	def joinBySingleProperty(property: String, dbpedia: RDD[Subject], wikidata: RDD[Subject]): RDD[(UUID, UUID)] = {
		val propertyKeyDBpedia = keyBySingleProperty(dbpedia, property)
		val propertyKeyWikidata = keyBySingleProperty(wikidata, property)
		propertyKeyDBpedia
			.join(propertyKeyWikidata)
			.values
			.map { case (dbpediaSubject, wikidataSubject) =>
				(dbpediaSubject.id, wikidataSubject.id)
			}
	}

	/**
	  * Join DBpedia and Wikidata together based on ids
	  * @param dbpedia Subjects from DBpedia
	  * @param wikidata Subjects from Wikidata
	  * @return RDD containing pairs of UUIDs
	  */
	def join(dbpedia: RDD[Subject], wikidata: RDD[Subject]): RDD[(UUID, UUID)] = {
		val dbpediaIdJoined = joinBySingleProperty("id_dbpedia", dbpedia, wikidata)
		val wikidataIdJoined = joinBySingleProperty("id_wikidata", dbpedia, wikidata)
		dbpediaIdJoined.union(wikidataIdJoined).distinct
	}
}
