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

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import scala.util.matching.Regex
import scala.io.Source
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import scala.xml.XML

/**
  * Import-Job for importing DBpedia Triples to the dbpedia table.
  */
class DBpediaImport extends SparkJob {
	import DBpediaImport._
	appName = "DBpediaImport_v1.0"
	val keyspace = "wikidumps"
	val tablename = "dbpedia"

	var dbpediaDump: RDD[String] = _
	var dbpediaEntities: RDD[DBpediaEntity] = _

	// $COVERAGE-OFF$
	/**
	  * Loads DBpedia turtle dump from the HDFS.
	  * @param sc   Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		dbpediaDump = sc.textFile("dbpedia_de_clean.ttl")
	}

	/**
	  * Saves the DBpedia entities to the Cassandra.
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		dbpediaEntities.saveToCassandra(keyspace, tablename)
	}
	// $COVERAGE-ON$

	/**
	  * Parses the DBpedia turtle dump to DBpedia Entities.
	  * @param sc    Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val prefixes = loadPrefixes()

		val rdfTypFile = Source.fromURL(this.getClass.getResource("/rdf_types.xml"))
		val rdfTypes = XML.loadString(rdfTypFile.getLines.mkString("\n"))
		val organisations = for {
			organisation <- (rdfTypes \\ "types" \ "organisation").toList
			label = (organisation \ "label").text
		} yield label

		dbpediaEntities = dbpediaToCleanedTriples(dbpediaDump, prefixes)
			.map { case List(a, b, c) => (a, List((b, c))) }
			.filter { case (subject, propertyData) =>
				val resourceRegex = new Regex("(dbr|dbpedia-de):")
				resourceRegex.findFirstIn(subject).isDefined
			}.reduceByKey(_ ++ _)
			.map { case (subject, propertyData) =>
				extractProperties(subject, propertyData, organisations)
			}
	}
}

object DBpediaImport {
	/**
	  * Loads DBpedia prefixe tuples from a file.
	  * @return List of DBpedia prefix tuples used to shorten the DBpedia URLs
	  */
	def loadPrefixes(): List[(String, String)] = {
		Source.fromURL(getClass.getResource("/prefixes.txt"))
			.getLines
			.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))
	}

	/**
	  * Parses DBpedia triples into cleaned triples
	  *
	  * @param dbpedia RDD of Strings containing the url triples
	  * @param prefixes List of the url/prefix-Mapping as tuples (url, prefix)
	  * @param regexStr Regex which is removed from the resulting triple elements
	  * @return RDD of the cleaned triples
	  */
	def dbpediaToCleanedTriples(
		dbpedia: RDD[String],
		prefixes: List[(String, String)],
		regexStr: String = ""
	): RDD[List[String]] = {
		dbpedia
			.map(tokenize)
			.map(_.map { listEntry =>
				val dataString = listEntry.replaceAll("""[<>\"]""", "")
				prefixes
					.foldRight(dataString)((pair, data) => data.replace(pair._1, pair._2))
					.replaceAll(regexStr, "")
			})

	}

	/**
	  * Splits a triple into its components. The triple is split by the closing chevron. If the resulting list of
	  * elements contains more then three elements only the first three are taken. If the resulting list contains less
	  * than three elements it is filled up with empty Strings.
	  *
	  * @param tripleString Turtle Triple
	  * @return List of the tripe components (three elements)
	  */
	def tokenize(tripleString: String): List[String] = {
		val elementList = tripleString
			.split("> ")
			.map(_.trim)
			.filter(_ != ".")
			.map(_.replaceAll("\\A<", ""))
			.toList
		val filteredList = elementList.filter(_.nonEmpty)
		(elementList.size, filteredList.size) match {
			case (3, x) => elementList
			case (x, 3) => filteredList
			case _ => elementList.slice(0, 3) ++ (0 until (3 - elementList.length)).map(t => "")
		}
	}

	/**
	  * Extracts the wikidata Id if present
	  *
	  * @param list owl:sameAs List
	  * @return The wikidata id or None if the id could not be found
	  */
	def extractWikidataId(list: List[String]): Option[String] = {
		val prefix = "wikidata:"
		val idOption = list.find(_.startsWith(prefix))
		idOption.map(id => id.drop(prefix.length))
	}

	/**
	  * Extracts the instancetype from a given list.
	  *
	  * @param list          rdf:type List
	  * @param organisations List of all organisation subclasses
	  * @return The instancetype or None if no organisation subclass could be found
	  */
	def extractInstancetype(list: List[String], organisations: List[String]): Option[String] = {
		val instanceTypes = for {
			item <- list
			if item.startsWith("dbo:")
			// To remove "dbo:" prefix
			rdfType = item.drop(4)
			if organisations.contains(rdfType)
		} yield rdfType

		instanceTypes.headOption
	}

	/**
	  * Translates a set of Triples grouped by their subject to a DBpediaEntitiy.
	  *
	  * @param subject      the subject of the Triples
	  * @param propertyData List of Predicates and Objects of the Triples as tuples (predicate, object)
	  * @return DBpediaEntity containing all the data from the Triples
	  */
	def extractProperties(
		subject: String,
		propertyData: List[(String, String)],
		organisations: List[String]
	): DBpediaEntity = {
		val name = subject.replaceAll("dbpedia-de:", "")
		val entity = DBpediaEntity(name)
		val dbpediaPropertyName = Map[String, String](
			"wikipageid" -> "dbo:wikiPageID",
			"wikidataid" -> "owl:sameAs",
			"label" -> "rdfs:label",
			"description" -> "dbo:abstract",
			"instancetype" -> "rdf:type"
		)
		val data = propertyData
			.groupBy(_._1)
			.mapValues(_.map(_._2).toList)

		entity.wikipageid = data.getOrElse(dbpediaPropertyName("wikipageid"), List()).headOption
		entity.wikidataid = extractWikidataId(
			data.getOrElse(dbpediaPropertyName("wikidataid"), List())
		)
		entity.label = data.getOrElse(dbpediaPropertyName("label"), List()).headOption
		entity.description = data.getOrElse(dbpediaPropertyName("description"), List()).headOption
		entity.instancetype = extractInstancetype(
			data.getOrElse(dbpediaPropertyName("instancetype"), List()),
			organisations)

		// Remove values from data which are empty or contain only one element
		val redundant = dbpediaPropertyName.values
			.filter(value => data.getOrElse(value, Nil).length < 2)
		entity.data = data -- redundant

		entity
	}
}
