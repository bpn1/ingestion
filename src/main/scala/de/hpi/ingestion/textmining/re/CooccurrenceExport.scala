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
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Exports the cooccurrences to Neo4j CSV. (This is just for debugging and no necessary part of the pipeline.)
  */
class CooccurrenceExport extends SparkJob {
	appName = "Co-Occurrence Export"
	configFile = "textmining.xml"
	val separator = ","
	val quote = "\""
	sparkOptions("spark.yarn.executor.memoryOverhead") = "4096"

	var cooccurrences: RDD[Cooccurrence] = _
	var relations: RDD[Relation] = _
	var nodes: RDD[String] = _
	var edges: RDD[String] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Cooccurrences from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		cooccurrences = sc.cassandraTable[Cooccurrence](settings("keyspace"), settings("cooccurrenceTable"))
		relations = sc.cassandraTable[Relation](settings("keyspace"), settings("DBpediaRelationTable"))
	}

	/**
	  * Saves the CSV files to the HDFS.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		nodes.saveAsTextFile(s"relation_nodes_${System.currentTimeMillis()}")
		edges.saveAsTextFile(s"relation_edges_${System.currentTimeMillis()}")
	}
	// $COVERAGE-ON$

	/**
	  * Exports the Co-Occurrence nodes and edges to CSV.
	  * (This is just for debugging and no necessary part of the pipeline.)
	  * @param sc    Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val cleanedCooccurrences = cooccurrences.map { occurrence =>
			val cleanedEntities = occurrence.entitylist.map(_.replaceAll("\"", "\\\\\""))
			occurrence.copy(entitylist = cleanedEntities)
		}
		val cleanedRelations = relations.map { relation =>
			val cleanedSubject = relation.subjectentity.replaceAll("\"", "\\\\\"").trim
			val cleanedObject = relation.objectentity.replaceAll("\"", "\\\\\"").trim
			relation.copy(subjectentity = cleanedSubject, objectentity = cleanedObject)
		}
		val nodesCooc = cleanedCooccurrences
			.flatMap(_.entitylist.map(_.trim))

		val edgesCooc = cleanedCooccurrences
			.flatMap { case Cooccurrence(entities, count) =>
				entities.asymSquare().map((_, count))
			}.reduceByKey(_ + _)
			.map { case ((start, end), count) =>
				s"${quote}${start}${quote},${count},${quote}${end}${quote},CO_OCCURRENCE"
			}.distinct
		val nodesRel = cleanedRelations.flatMap(rel => List(rel.subjectentity.trim, rel.objectentity.trim))
		nodes = (nodesCooc ++ nodesRel)
			.map(node => s"${quote}${node}${quote},${quote}${node}${quote},Entity")
			.distinct
		val edgesRel = cleanedRelations
			.map { case Relation(subjectentity, relationtype, objectentity) =>
				s"${quote}${subjectentity}${quote},${relationtype},${quote}${objectentity}${quote},DBPEDIA"
			}.distinct
		edges = edgesCooc ++ edgesRel
	}
}
