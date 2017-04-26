package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity
import de.hpi.ingestion.datalake.models.Version
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.xml.XML

object TestData {
	// scalastyle:off line.size.limit

	def prefixesList: List[(String,String)] = {
		val prefixFile = Source.fromURL(getClass.getResource("/prefixes.txt"))
		val prefixes = prefixFile.getLines.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))
		prefixes
	}

	def organisations: List[String] = {
		val rdfTypFile = Source.fromURL(this.getClass.getResource("/rdf_types.xml"))
		val rdfTypes = XML.loadString(rdfTypFile.getLines.mkString("\n"))
		for {
			organisation <- (rdfTypes \\ "types" \ "organisation").toList
			label = (organisation \ "label").text
		} yield label
	}

	def line: String = """<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> ."""

	def shorterLineList: List[String] = List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> .""",
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> .""",
			""
	)

	def longerLineList: List[String] = List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> .""",
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://purl.org/dc/terms/subject> ."""
	)

	def lineTokens: List[String] = List(
		"http://de.dbpedia.org/resource/Anschluss_(Soziologie)",
		"http://purl.org/dc/terms/subject",
		"http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie"
	)

	def properties: List[(String, String)] = List(
		("dbo:wikiPageID", "1"),
		("owl:sameAs", "wikidata:Q1"),
		("owl:sameAs", "yago:X"),
		("rdfs:label", "Anschluss"),
		("dbo:abstract", "Lorem Ipsum"),
		("rdf:type", "dbo:Organisation"),
		("rdf:type", "dbo:Company"),
		("rdf:type", "dbo:Airline"),
		("dbp:founded", "1926-04-15"),
		("dbp:founded", "Chicago, Illinois"),
		("dbp:headquarters", "CentrePort, Fort Worth, Texas, United States")
	)

	def parsedEntity(name: String) = DBPediaEntity(
		name,
		Option("1"),
		Option("Q1"),
		Option("Anschluss"),
		Option("Lorem Ipsum"),
		Option("Company"),
		Map(
			"rdf:type" -> List("dbo:Organisation", "dbo:Company", "dbo:Airline"),
			"dbp:founded" -> List("1926-04-15", "Chicago, Illinois"),
			"dbp:headquarters" -> List("CentrePort, Fort Worth, Texas, United States"),
			"owl:sameAs" -> List("wikidata:Q1", "yago:X")
		)
	)

	def turtleRDD(sc: SparkContext): RDD[String] = {
		sc.parallelize(List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/V> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Autor> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/V> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Wikipedia:Liste> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/T> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Autor> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/T> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Wikipedia:Liste> ."""
		))
	}

	def tripleRDD(sc: SparkContext): RDD[(String, (String, String))] = {
		sc.parallelize(List(
			("dbpedia-de:Anschluss_(Soziologie)", ("dct:subject", "dbpedia-de:Kategorie:Soziologische_Systemtheorie")),
			("dbpedia-de:Liste_von_Autoren/V", ("dct:subject", "dbpedia-de:Kategorie:Autor")),
			("dbpedia-de:Liste_von_Autoren/V", ("dct:subject", "dbpedia-de:Kategorie:Wikipedia:Liste")),
			("dbpedia-de:Liste_von_Autoren/T", ("dct:subject", "dbpedia-de:Kategorie:Autor")),
			("dbpedia-de:Liste_von_Autoren/T", ("dct:subject", "dbpedia-de:Kategorie:Wikipedia:Liste"))
		))
	}

	def entityRDD(sc: SparkContext): RDD[DBPediaEntity] = {
		sc.parallelize(List(
			DBPediaEntity(dbpedianame="Liste_von_Autoren/V", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste"))),
			DBPediaEntity(dbpedianame="Anschluss_(Soziologie)", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Soziologische_Systemtheorie"))),
			DBPediaEntity(dbpedianame="Liste_von_Autoren/T", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste")))
		))
	}

	def version(sc: SparkContext): Version = Version("DBPediaDataLakeImport", datasources = List("dataSources"), sc)

	def testEntity: DBPediaEntity = DBPediaEntity(
		dbpedianame = "dbpedia-de:List_von_Autoren",
		label = Option("Liste von Autoren"),
		data = Map(
			"wikidata_id" -> List("Q123"),
			"dbo:viafId" -> List("X123"),
			"property-de:viaf" -> List("Y123"),
			"geo:lat" -> List("52"),
			"property-de:latitude" -> List("53"),
			"geo:long" -> List("100"),
			"testProperty" -> List("test")
		)
	)

	def mapping: Map[String, List[String]] = Map(
		"id_wikidata" -> List("wikidata_id"),
		"id_dbpedia" -> List("dbpedianame"),
		"id_wikipedia" -> List("dbpedianame"),
		"id_viaf" -> List("dbo:viafId", "property-de:viaf"),
		"geo_coords_lat" -> List("geo:lat", "property-de:latitude", "property-de:breitengrad"),
		"geo_coords_long" -> List("geo:long", "property-de:longitude", "property-de:längengrad")
	)
	// scalastyle:on line.size.limit
}
