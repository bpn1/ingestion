package de.hpi.ingestion.curation

import java.util.UUID

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsArray, JsValue, Json}

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
			|	},
			| "updated": {
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
			|				"gen_sectors": ["C", "G", "C29", "G45", "HELLO"],
			|				"http://purl.org/linguistics/gold/hypernym": ["dbpedia-de:Hersteller"],
			|				"id_dbpedia": ["Établissements Alma"],
			|				"id_wikidata": ["UPDATED"],
			|				"id_wikipedia": ["Établissements Alma"],
			|				"owl:sameAs": ["freebase:m.02w6q9j", "wikidata:Q288669", "dbpedia-wikidata:Q288669", "dbr:Alma_(French_automobile)"],
			|				"property-de:auflösungsdatum": ["1929^^xsd:integer"],
			|				"property-de:branche": ["dbpedia-de:Kraftfahrzeughersteller"],
			|				"property-de:gründungsdatum": ["1926^^xsd:integer", "LOL"],
			|				"property-de:leitung": ["Vaslin, Maurice Coquet@de ."],
			|				"property-de:name": ["Établissements Alma", "WULU"],
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
		val templateVersion = Version(appName, List("human"), sc, false, Option("subject"))

		val json = Json.parse(commit)
		val created = extractMap(json, List("created")).values
		val updated = extractMap(json, List("updated")).values
		val deleted = extractMap(json, List("deleted")).values

		val createdSubjects = created.map(subJson => createSubject(subJson, templateVersion))
		val updatedMasterIds: List[UUID] =
			updated.map(subJson => UUID.fromString(extractString(subJson, List("master")).get)).toList
		println(updatedMasterIds)

		val subjects = input.take(1).fromAnyRDD[Subject]().head
		val updatedMasterSubjects =
			subjects.filter(subject => updatedMasterIds.contains(subject.master) && subject.datasource == "dbpedia")

		val updatedSubjects = updated
			.map(sub => (updatedMasterSubjects.filter(subject =>
				subject.master == UUID.fromString(extractString(sub, List("master")).get)).first, sub))
		  	.map{case (oldSubject: Subject, json: JsValue) => updateSubject(oldSubject, json, templateVersion)}

		List(subjects).toAnyRDD()
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

	def createSubject(json: JsValue, templateVersion: Version): Subject = {
		val id = UUID.fromString(extractString(json, List("id")).get)
		val subject = Subject.empty(id = id, datasource = "human")

		val sm = new SubjectManager(subject, templateVersion)
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

	def updateSubject(oldSubject: Subject, json: JsValue, templateVersion: Version): Subject = {
		val id = UUID.fromString(extractString(json, List("id")).get)
		val subject = Subject.empty(id = id, datasource = "human")
		val sm = new SubjectManager(subject, templateVersion)

		val name: Option[String] = extractString(json, List("name"))
		val aliases: List[String] = getValue(json, List("aliases")).get match {
			case arr: JsArray => arr.as[List[String]]
			case _  => Nil
		}
		val category: Option[String] = extractString(json, List("category"));
		val properties: Map[String, List[String]] =
			extractMap(json, List("properties")).map{case (key, value) => (key, value.as[List[String]])}
		val relations: Map[UUID, Map[String, String]] = Map()

		sm.setMaster(oldSubject.master)

		if (name != oldSubject.name) {
			sm.setName(name)
		}

		if (category != oldSubject.category) {
			sm.setCategory(category)
		}

		if (properties != oldSubject.properties) {
			println("not equal")
			val diff = (properties.toSet diff oldSubject.properties.toSet).toMap
			println(diff)
			// all changed properties are set and overwrites human properties
			sm.overwriteProperties(diff)
		}

		subject
	}


}
