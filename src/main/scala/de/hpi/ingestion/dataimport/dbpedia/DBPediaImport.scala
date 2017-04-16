package de.hpi.ingestion.dataimport.dbpedia

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import scala.util.matching.Regex
import scala.io.Source
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity

/**
  * Import-Job for importing DBPedia Triples to the dbpedia table.
  */
object DBPediaImport {
	val appName = "DBPediaImport_v1.0"
	val dataSources = List("dbpedia_20161203")
	val keyspace = "wikidumps"
	val tablename = "dbpedia"

	/**
	  * Splits a triple into its components. The triple is split by the closing chevron. If the resulting list of
	  * elements contains more then three elements only the first three are taken. If the resulting list contains less
	  * than three elements it is filled up with empty Strings.
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
		if(elementList.size == 3) {
			elementList
		} else if(filteredList.size == 3) {
			filteredList
		} else {
			if(elementList.size > 3) {
				elementList.slice(0, 3)
			} else {
				elementList match {
					case List(a, b) => List(a, b, "")
					case List(a) => List(a, "", "")
					case List() => List("", "", "")
				}
			}
		}
	}

	/**
	  * Replaces a url by a prefix.
	  * @param str String containing the url
	  * @param prefixes List of the url/prefix-Mapping as tuples (url, prefix)
	  * @return Cleaned String
	  */
	def cleanURL(str: String, prefixes: List[(String,String)]): String = {
		var dataString = str.replaceAll("""[<>\"]""", "")
		for (pair <- prefixes) {
			dataString = dataString.replace(pair._1, pair._2)
		}
		dataString
	}

	/**
	  * Translates a set of Triples grouped by their subject to a DBpediaEntitiy.
	  * @param subject the subject of the Triples
	  * @param propertyData List of Predicates and Objects of the Triples as tuples (predicate, object)
	  * @return DBPediaEntity containing all the data from the Triples
	  */
	def extractProperties(subject: String, propertyData: List[(String, String)]): DBPediaEntity = {
		val entity = DBPediaEntity(subject)
		val dbpediaPropertyName = Map[String, String](
			"wikipageid" -> "dbo:wikiPageID",
			"label" -> "rdfs:label",
			"description" -> "dbo:abstract",
			"instancetype" -> "rdf:type")
		val data = propertyData
			.groupBy(_._1)
			.mapValues(_.map(_._2).toList)

		entity.wikipageid = data.getOrElse(dbpediaPropertyName("wikipageid"), List()).headOption
		entity.label = data.getOrElse(dbpediaPropertyName("label"), List()).headOption
		entity.description = data.getOrElse(dbpediaPropertyName("description"), List()).headOption
		entity.instancetype = data.getOrElse(dbpediaPropertyName("instancetype"), List()).headOption
		entity.data = data -- dbpediaPropertyName.values
		entity
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName(appName)
		val sc = new SparkContext(conf)

		val prefixFile = Source.fromURL(getClass.getResource("/prefixes.txt"))
		val prefixes = prefixFile.getLines.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))
		val prefixesBroadcast = sc.broadcast(prefixes)

		val dbpedia = sc.textFile("dbpedia_de_clean.ttl")

		dbpedia
			.map(tokenize)
			.mapPartitions({ rddPartition =>
				val prefixList = prefixesBroadcast.value
				rddPartition.map(_.map(listEntry => cleanURL(listEntry, prefixList)))
			}, true)
			.map { case List(a, b, c) => (a, (b, c)) }
			.filter { case (subject, (predicate, property)) =>
				val resourceRegex = new Regex("(dbr|dbpedia-de):")
				resourceRegex.findFirstIn(subject).isDefined
			}.groupByKey
			.map { case (subject, propertyData) =>
				extractProperties(subject, propertyData.toList)
			}
			.saveToCassandra(keyspace, tablename)

		sc.stop()
	}
}
