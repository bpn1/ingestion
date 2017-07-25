package de.hpi.ingestion.curation

import java.util.UUID

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsArray, JsValue, Json}
import spire.random.DistRng

object Commit extends SparkJob with JSONParser[(List[Subject], List[Subject], List[Subject])] {
	appName = "Commit"

	/**
		* Loads a number of input RDDs used in the job.
		*
		* @param sc   Spark Context used to load the RDDs
		* @param args arguments of the program
		* @return List of RDDs containing the data processed in the job.
		*/
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		println(args(0))
		val subjects = sc.cassandraTable[Subject]("datalake", "subject")
		List(subjects).toAnyRDD()
	}

	val commit =
		"""
			|{
			|	"deleted": {},
			|	"updated": {},
			|	"created": {
			|		"1e8183f0-49c3-4da6-92e9-b6f54982a19a": {
			|			"master": "1e8183f0-49c3-4da6-92e9-b6f54982a19a",
			|			"id": "1e8183f0-49c3-4da6-92e9-b6f54982a19a",
			|			"datasource": "dbpedia",
			|			"name": "Établissements Alma",
			|			"aliases": ["Alma"],
			|			"category": "business",
			|			"properties": {
			|				"dbo:industry": ["dbpedia-de:Kraftfahrzeughersteller"],
			|				"dbo:locationCity": ["dbpedia-de:Courbevoie"],
			|				"dbo:locationCountry": ["dbpedia-de:Courbevoie"],
			|				"dbo:wikiPageLength": ["2628^^xsd:nonNegativeInteger"],
			|				"dbo:wikiPageOutDegree": ["15^^xsd:nonNegativeInteger"],
			|				"dbo:wikiPageRevisionID": ["141737859^^xsd:integer"],
			|				"dbo:wikiPageWikiLink": ["dbpedia-de:Courbevoie", "dbpedia-de:Kraftfahrzeughersteller", "dbpedia-de:Frankreich", "dbpedia-de:Automobil", "dbpedia-de:Sechszylinder", "dbpedia-de:Hubraum", "dbpedia-de:OHV-Ventilsteuerung", "dbpedia-de:Ventilsteuerung", "dbpedia-de:Karosseriebauform", "dbpedia-de:Coupé", "dbpedia-de:Kategorie:Ehemaliger_Automobilhersteller_(Frankreich)", "dbpedia-de:Kategorie:Courbevoie", "dbpedia-de:Kategorie:Ehemaliges_Unternehmen_(Île-de-France)", "dbpedia-de:Kategorie:Gegründet_1926"],
			|				"dct:subject": ["dbpedia-de:Kategorie:Ehemaliger_Automobilhersteller_(Frankreich)", "dbpedia-de:Kategorie:Courbevoie", "dbpedia-de:Kategorie:Ehemaliges_Unternehmen_(Île-de-France)", "dbpedia-de:Kategorie:Gegründet_1926"],
			|				"foaf:isPrimaryTopicOf": ["http://de.wikipedia.org/wiki/Établissements_Alma"],
			|				"foaf:name": ["Établissements Alma@de ."],
			|				"gen_sectors": ["C", "G", "C29", "G45"],
			|				"http://purl.org/linguistics/gold/hypernym": ["dbpedia-de:Hersteller"],
			|				"id_dbpedia": ["Établissements Alma"],
			|				"id_wikidata": ["Q288669"],
			|				"id_wikipedia": ["Établissements Alma"],
			|				"owl:sameAs": ["freebase:m.02w6q9j", "wikidata:Q288669", "dbpedia-wikidata:Q288669", "dbr:Alma_(French_automobile)"],
			|				"property-de:auflösungsdatum": ["1929^^xsd:integer"],
			|				"property-de:branche": ["dbpedia-de:Kraftfahrzeughersteller"],
			|				"property-de:gründungsdatum": ["1926^^xsd:integer"],
			|				"property-de:leitung": ["Vaslin, Maurice Coquet@de ."],
			|				"property-de:name": ["Établissements Alma@de ."],
			|				"property-de:sitz": ["dbpedia-de:Courbevoie"],
			|				"property-de:wikiPageUsesTemplate": ["dbpedia-de:Vorlage:Infobox_Unternehmen", "dbpedia-de:Vorlage:Navigationsleiste_Französische_Automobilmarken_von_1919_bis_1930"],
			|				"prov:wasDerivedFrom": ["http://de.wikipedia.org/wiki/Établissements_Alma?oldid=141737859"],
			|				"rdf:type": ["dbo:Company", "dbo:Organisation", "schema:Organization", "dul:#SocialPerson", "wikidata:Q43229", "dbo:Agent", "dul:#Agent", "owl:Thing"],
			|				"rdfs:comment": ["Établissements Alma war ein französischer Hersteller von Automobilen.@de ."]
			|			},
			|			"relations": null,
			|			"selected": true
			|		}
			|	}
			|}
		""".stripMargin

	/**
		* Executes the data processing of this job and produces the output data.
		*
		* @param input List of RDDs containing the input data
		* @param sc    Spark Context used to e.g. broadcast variables
		* @param args  arguments of the program
		* @return List of RDDs containing the output data
		*/
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val json = Json.parse(commit)
		val created = extractMap(json, List("created"))
		val updated = extractMap(json, List("updated"))
		val deleted = extractMap(json, List("deleted"))
		val createdSubjects = created.map(sub => createSubject(sub._2, sc))
		val subjects = input.take(1).fromAnyRDD[Subject]()
		subjects.toAnyRDD()
	}

	/**
		* Saves the output data to e.g. the Cassandra or the HDFS.
		*
		* @param output List of RDDs containing the output of the job
		* @param sc     Spark Context used to connect to the Cassandra or the HDFS
		* @param args   arguments of the program
		*/
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {

	}

	/**
		* Extracts the JSON data from a JSON object to an Entity.
		*
		* @param json JSON-Object containing the data
		* @return Entity containing the parsed data
		*/
	override def fillEntityValues(json: JsValue): (List[Subject], List[Subject], List[Subject]) = {

		(List[Subject](), List[Subject](), List[Subject]())
	}

	def createSubject(json: JsValue, sc: SparkContext): Subject = {
		val id = UUID.fromString(extractString(json, List("id")).get)
		val subject = Subject.empty(id = id, datasource = "human")
		val version = Version(appName, List(), sc, false, Option("subject"))
		val sm = new SubjectManager(subject, version)
		sm.setMaster(UUID.fromString(extractString(json, List("master")).get))
		sm.setName(extractString(json, List("name")))
		sm.setAliases(getValue(json, List("aliases")).get match {
			case arr: JsArray => arr.as[List[String]]
			case _  => Nil
		})
		sm.setCategory(extractString(json, List("category")))
		sm.setProperties(extractMap(json, List("properties")).map{case (key, value) => (key, value.as[List[String]])})
		sm.setRelations(Map())
		subject
	}

	def updateSubject(oldSubject: Subject, json: JsValue, sc: SparkContext): Subject = {
		val id = UUID.fromString(extractString(json, List("id")).get)
		val subject = Subject.empty(id = id, datasource = "human")
		val version = Version(appName, List(), sc, false, Option("subject"))
		val sm = new SubjectManager(subject, version)
		sm.setMaster(UUID.fromString(extractString(json, List("master")).get))
		sm.setName(extractString(json, List("name")))
		sm.setAliases(getValue(json, List("aliases")).get match {
			case arr: JsArray => arr.as[List[String]]
			case _  => Nil
		})
		sm.setCategory(extractString(json, List("category")))
		sm.setProperties(extractMap(json, List("properties")).map{case (key, value) => (key, value.as[List[String]])})
		sm.setRelations(Map())
		subject
	}
}
