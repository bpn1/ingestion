package de.hpi.ingestion.dataimport.dbpedia

import org.apache.spark.SparkContext
import com.datastax.spark.connector._

import scala.util.matching.Regex
import scala.io.Source
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd.RDD

import scala.xml.XML

/**
  * Import-Job for importing DBpedia Triples to the dbpedia table.
  */
object DBpediaImport extends SparkJob {
	appName = s"DBpediaImport_v1.0_${System.currentTimeMillis()}"
	val dataSources = List("dbpedia_20161203")
	val keyspace = "wikidumps"
	val tablename = "dbpedia"

	// $COVERAGE-OFF$
	/**
	  * Loads DBpedia turtle dump from the HDFS.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.textFile("dbpedia_de_clean.ttl")
		List(dbpedia).toAnyRDD()
	}

	/**
	  * Saves the DBpedia entities to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[DBpediaEntity]()
			.head
			.saveToCassandra(keyspace, tablename)
	}
	// $COVERAGE-ON$

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
	  * Extracts the wikidata Id if present
	  * @param list owl:sameAs List
	  * @return The wikidata id or None if the id could not be found
	  */
	def extractWikiDataId(list: List[String]): Option[String] = {
		val prefix = "wikidata:"
		val idOption = list.find(_.startsWith(prefix))
		idOption.map(id => id.drop(prefix.length))
	}

	/**
	  * Extracts the instancetype from a given list.
	  * @param list rdf:type List
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
	  * @param subject the subject of the Triples
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
			"instancetype" -> "rdf:type")
		val data = propertyData
			.groupBy(_._1)
			.mapValues(_.map(_._2).toList)

		entity.wikipageid = data.getOrElse(dbpediaPropertyName("wikipageid"), List()).headOption
		entity.wikidataid = extractWikiDataId(
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

	/**
	  * Parses the DBpedia turtle dump to DBpedia Entities.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val dbpedia = input.fromAnyRDD[String]().head

		val prefixFile = Source.fromURL(getClass.getResource("/prefixes.txt"))
		val prefixes = prefixFile.getLines.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))

		val rdfTypFile = Source.fromURL(this.getClass.getResource("/rdf_types.xml"))
		val rdfTypes = XML.loadString(rdfTypFile.getLines.mkString("\n"))
		val organisations = for {
			organisation <- (rdfTypes \\ "types" \ "organisation").toList
			label = (organisation \ "label").text
		} yield label


		val entities = dbpedia
			.map(tokenize)
			.map(_.map(listEntry => cleanURL(listEntry, prefixes)))
			.map { case List(a, b, c) => (a, List((b, c))) }
			.filter { case (subject, propertyData) =>
				val resourceRegex = new Regex("(dbr|dbpedia-de):")
				resourceRegex.findFirstIn(subject).isDefined
			}.reduceByKey(_ ++ _)
			.map { case (subject, propertyData) =>
				extractProperties(subject, propertyData, organisations)
			}
		List(entities).toAnyRDD()
	}
}
