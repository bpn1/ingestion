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

package de.hpi.ingestion.dataimport.dbpedia

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.dataimport.dbpedia.DBpediaImport.{dbpediaToCleanedTriples, loadPrefixes}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Extracts all relations from english dbpedia and translates the english title to the german ones.
  */
class DBpediaRelationParser extends SparkJob {
	import DBpediaRelationParser._
	appName = "DBpediaRelationParser"
	configFile = "textmining.xml"

	var dbpediaTtl: RDD[String] = _
	var labelTtl: RDD[String] = _
	var relations: RDD[Relation] = _

	// $COVERAGE-OFF$
	/**
	  * Loads triples of relations and english to german translation from ttl files.
	  * @param sc   Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		dbpediaTtl = sc.textFile("mappingbased_objects_en.ttl")
		labelTtl = sc.textFile("interlanguage_links_en.ttl")
	}

	/**
	  * Saves relations to cassandra.
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		relations.saveToCassandra(settings("keyspace"), settings("DBpediaRelationTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Extracts all relations from English DBpedia and translates the English title to the German ones.
	  * @param sc    Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val prefixes = loadPrefixes()
		val germanLabels = getGermanLabels(labelTtl, prefixes)
		val gerLabelsBroadcast = sc.broadcast(germanLabels)
		relations = dbpediaToCleanedTriples(dbpediaTtl, prefixes, "dbo:|dbr:")
			.mapPartitions({ entryPartition =>
				val localGerLabels = gerLabelsBroadcast.value
				entryPartition.map { case List(subj, rel, obj) =>
					val gerSubj = localGerLabels.getOrElse(subj, subj)
					val gerObj = localGerLabels.getOrElse(obj, obj)
					Relation(gerSubj.replaceAll("_", " "), rel, gerObj.replaceAll("_", " "))
				}
			})
	}
}

object DBpediaRelationParser {
	/**
	  * Parses only English to German translations from DBpedia triples.
	  *
	  * @param labels RDD of triples containing the same as relation
	  * @return Map from english wikipedia titles to their german counterparts
	  */
	def getGermanLabels(labels: RDD[String], prefixes: List[(String, String)]): Map[String, String] = {
		dbpediaToCleanedTriples(labels, prefixes)
			.filter { case List(subj, rel, obj) => rel.startsWith("owl:") && obj.startsWith("dbpedia-de:") }
			.map(_.map(_.replaceAll("dbo:|dbpedia-de:|dbr:", "")))
			.map { case List(subj, rel, obj) => subj -> obj }
			.collect
			.toMap
	}
}
