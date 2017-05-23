package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
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

	def parsedEntity(name: String) = DBpediaEntity(
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

	def entityRDD(sc: SparkContext): RDD[DBpediaEntity] = {
		sc.parallelize(List(
			DBpediaEntity(dbpedianame="Liste_von_Autoren/V", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste"))),
			DBpediaEntity(dbpedianame="Anschluss_(Soziologie)", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Soziologische_Systemtheorie"))),
			DBpediaEntity(dbpedianame="Liste_von_Autoren/T", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste")))
		))
	}

	def version(sc: SparkContext): Version = Version("DBpediaDataLakeImport", datasources = List("dataSources"), sc)

	def testEntity: DBpediaEntity = DBpediaEntity(
		dbpedianame = "dbpedia-de:List_von_Autoren",
		label = Option("Liste von Autoren"),
		data = Map(
			"wikidata_id" -> List("Q123"),
			"dbo:viafId" -> List("X123"),
			"property-de:viaf" -> List("Y123"),
			"geo:lat" -> List("52"),
			"property-de:latitude" -> List("53"),
			"geo:long" -> List("100"),
			"dbo:country" -> List("Koblenz@de ."),
			"property-de:mitarbeiteranzahl" -> List("12^^xsd:integer", "13^^xsd:nonNegativeInteger"),
			"dbo:industry" -> List("dbpedia-de:Kraftfahrzeughersteller", "dbpedia-de:Brauerei"),
			"testProperty" -> List("test")
		)
	)

	def mapping: Map[String, List[String]] = Map(
		"id_wikidata" -> List("wikidata_id"),
		"id_dbpedia" -> List("dbpedianame"),
		"id_wikipedia" -> List("dbpedianame"),
		"id_viaf" -> List("dbo:viafId", "property-de:viaf"),
		"geo_coords_lat" -> List("geo:lat", "property-de:latitude", "property-de:breitengrad"),
		"geo_coords_long" -> List("geo:long", "property-de:longitude", "property-de:längengrad"),
		"geo_country" -> List("dbo:country", "property-de:land"),
		"gen_employees" -> List("dbo:numberOfEmployees", "property-de:mitarbeiteranzahl"),
		"gen_sectors" -> List("dbo:industry")
	)

	def strategies: Map[String, List[String]] = Map(
		"Kraftfahrzeughersteller" -> List("29", "45"),
		"Brauerei" -> List("11")
	)

	def unnormalizedEmployees: List[String] = List("27000^^xsd:integer", "27000^^xsd:nonNegativeInteger", "10^^xsd:nonNegativeInteger", "über 1000@de .", "1 Million")
	def normalizedEmployees: List[String] = List("27000", "10", "1000")
	def unnormalizedCoords: List[String] = List("48.34822^^xsd:float;10.905282^^xsd:float","48348220^^xsd:integer;10905282^^xsd:integer", "48.34822^^xsd:double;10.905282^^xsd:double")
	def normalizedCoords: List[String] = List("48.34822;10.905282")
	def unnormalizedCountries: List[String] = List("dbpedia-de:Japanisches_Kaiser-reich", "LI@de .", "Deutschland@de .", "dbpedia-de:Datei:Flag_of_Bavaria_(striped).svg", "15^^xsd:integer")
	def normalizedCountries: List[String] = List("Japanisches Kaiser reich", "Deutschland")
	def unnormalizedCities: List[String] = List("Frankfurt a.M.@de .", "Frankfurt/Main@de .", "London", "dbpedia-de:Berlin-Tegel")
	def normalizedCities: List[String] = List("Frankfurt a.M.", "Frankfurt/Main", "London", "Berlin Tegel")
	def unnormalizedSectors: List[String] = List("dbpedia-de:Kraftfahrzeughersteller", "dbpedia-de:Brauerei", "Unknown")
	def normalizedSectors: List[String] = List("Kraftfahrzeughersteller", "Brauerei")
	def unnormalizedDefaults: List[String] = List("very^^xsd:nonNegativeInteger", "generic@de .", "dbpedia-de:values", "even", "dash-containing^^xsd:float", "123^^xsd:integer", "b4ckf1sh")
	def normalizedDefaults: List[String] = List("very", "generic", "values", "even", "dash containing", "123", "b4ckf1sh")
	// scalastyle:on line.size.limit
}
