package de.hpi.ingestion.dataimport.dbpedia

import java.util.UUID

import de.hpi.ingestion.dataimport.SharedNormalizations

import scala.io.Source
import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.dataimport.dbpedia.models.{DBpediaEntity, Relation}
import de.hpi.ingestion.datalake.models.{Subject, Version}

// scalastyle:off number.of.methods
// scalastyle:off line.size.limit

object TestData {
	val datasource = "dbpedia"
	val idList = List.fill(6)(UUID.randomUUID())

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
			"",
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://purl.org/dc/terms/subject> <"""
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

	def unfilteredEntities: List[DBpediaEntity] = List(
		DBpediaEntity(dbpedianame = "D1", instancetype = Option("type 1")),
		DBpediaEntity(dbpedianame = "D2", instancetype = Option("type 2")),
		DBpediaEntity(dbpedianame = "D3", instancetype = Option("type 3")),
		DBpediaEntity(dbpedianame = "D4", instancetype = Option(null)),
		DBpediaEntity(dbpedianame = "D5")
	)

	def filteredEntities: List[DBpediaEntity] = List(
		DBpediaEntity(dbpedianame = "D1", instancetype = Option("type 1")),
		DBpediaEntity(dbpedianame = "D2", instancetype = Option("type 2")),
		DBpediaEntity(dbpedianame = "D3", instancetype = Option("type 3"))
	)

	def version(sc: SparkContext): Version = Version("DBpediaDataLakeImport", List("dataSources"), sc, false, None)

	def testEntity: DBpediaEntity = DBpediaEntity(
		dbpedianame = "dbpedia-de:List_von_Autoren",
		label = Option("Gesellschaft Liste von Autoren mbH@de ."),
		data = Map(
			"wikidata_id" -> List("Q123"),
			"dbo:viafId" -> List("X123"),
			"property-de:viaf" -> List("Y123"),
			"geo:lat" -> List("52"),
			"property-de:latitude" -> List("53"),
			"geo:long" -> List("100"),
			"dbo:country" -> List("Schweiz@de ."),
			"property-de:mitarbeiteranzahl" -> List("12^^xsd:integer", "13^^xsd:nonNegativeInteger"),
			"dbo:industry" -> List("dbpedia-de:Kraftfahrzeughersteller", "dbpedia-de:Brauerei"),
			"testProperty" -> List("test")
		)
	)

	def dbpediaEntities: List[DBpediaEntity] = List(
		DBpediaEntity(
			dbpedianame = "dbpedia-de:List_von_Autoren",
			label = Option("Liste von Autoren GmbH@de ."),
			instancetype = Option("Company"),
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
		),
		DBpediaEntity(dbpedianame = "dbpedia-de:Liste_von_Wurst")
	)

	def translatedSubjects: List[Subject] = List(
		Subject(master = null, datasource = "dbpedia", name = Option("Liste von Autoren GmbH"), category = Option("business")),
		Subject(master = null, datasource = "dbpedia")
	)

	def mapping: Map[String, List[String]] = Map(
		"id_wikidata" -> List("wikidata_id"),
		"id_dbpedia" -> List("dbpedianame"),
		"id_wikipedia" -> List("dbpedianame"),
		"id_viaf" -> List("dbo:viafId", "property-de:viaf"),
		"geo_coords_lat" -> List("geo:lat", "property-de:latitude", "property-de:breitengrad"),
		"geo_coords_long" -> List("geo:long", "property-de:longitude", "property-de:längengrad"),
		"geo_country" -> List("dbo:country"),
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
	def unnormalizedCountries: List[String] = List("dbpedia-de:Japanisches_Kaiser-reich", "LI@de .", "Deutschland@de .", "dbpedia-de:Datei:Flag_of_Bavaria_(striped).svg", "15^^xsd:integer", "dbpedia-de:AB")
	def normalizedCountries: List[String] = List("LI", "DE", "AB")
	def unnormalizedCities: List[String] = List("Frankfurt a.M.@de .", "Frankfurt/Main@de .", "London", "dbpedia-de:Berlin-Tegel")
	def normalizedCities: List[String] = List("Frankfurt a.M.", "Frankfurt/Main", "London", "Berlin Tegel")
	def unnormalizedSectors: List[String] = List("dbpedia-de:Kraftfahrzeughersteller", "dbpedia-de:Brauerei", "Unknown", "dbpedia-de:TestSector")
	def normalizedSectors: List[String] = List("Kraftfahrzeughersteller", "Brauerei", "TestSector")
	def mappedSectors: List[String] = List("29", "45", "11", "TestSector")
	def unnormalizedURLs: List[String] = List("https://youtube.de@de .", "http://facebook.de@de .", "http://www.google.de", "www.hans", "NotAURL@de .")
	def normalizedURLs: List[String] = List("https://youtube.de", "http://facebook.de", "http://www.google.de")
	def unnormalizedDefaults: List[String] = List("very^^xsd:nonNegativeInteger", "generic@de .", "dbpedia-de:values", "even", "dash-containing^^xsd:float", "123^^xsd:integer", "b4ckf1sh")
	def normalizedDefaults: List[String] = List("very", "generic", "values", "even", "dash containing", "123", "b4ckf1sh")

	def applyInput: List[List[String]] = List(
		this.unnormalizedCoords,
		this.unnormalizedCities,
		this.unnormalizedCountries,
		this.unnormalizedSectors,
		this.unnormalizedEmployees,
		this.unnormalizedURLs,
		List("default"),
		List("GmbH", "&", "Co", ".", "KG")
	)
	def applyAttributes: List[String] = List("geo_coords", "geo_city", "geo_country", "gen_sectors", "gen_employees", "gen_urls", "default", "gen_legal_form")
	def applyStrategies: List[(List[String] => List[String])] = List(
		DBpediaNormalizationStrategy.normalizeCoords,
		DBpediaNormalizationStrategy.normalizeCity,
		DBpediaNormalizationStrategy.normalizeCountry,
		DBpediaNormalizationStrategy.normalizeSector,
		DBpediaNormalizationStrategy.normalizeEmployees,
		DBpediaNormalizationStrategy.normalizeURLs,
		identity,
		SharedNormalizations.normalizeLegalForm
	)
	def unnormalizedAttributes: Map[String, List[String]] = Map(
		"gen_sectors" -> this.unnormalizedSectors,
		"geo_coords" -> this.unnormalizedCoords,
		"geo_country" -> this.unnormalizedCountries,
		"geo_city" -> this.unnormalizedCities,
		"gen_employees" -> this.unnormalizedEmployees,
		"gen_urls" -> this.unnormalizedURLs
	)
	def normalizedAttributes: Map[String, List[String]] = Map(
		"gen_sectors" -> this.mappedSectors,
		"geo_coords" -> this.normalizedCoords,
		"geo_country" -> this.normalizedCountries,
		"geo_city" -> this.normalizedCities,
		"gen_employees" -> this.normalizedEmployees,
		"gen_urls" -> this.normalizedURLs
	)

	def interlanguageLinksEn(): List[String] = List(
		"""<http://dbpedia.org/resource/Audi_EN> <http://www.w3.org/2002/07/owl#sameas> <http://de.dbpedia.org/resource/Audi_DE> .""",
		"""<http://dbpedia.org/resource/Volkswagen_EN> <http://www.w3.org/2002/07/owl#sameas> <http://de.dbpedia.org/resource/Volkswagen_DE> .""",
		"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> ."""
	)

	def germanLabels(): Map[String, String] = {
		Map("Audi_EN" -> "Audi_DE", "Volkswagen_EN" -> "Volkswagen_DE")
	}

	def dbpediaRawRelations(): List[String] = List(
		"""<http://dbpedia.org/resource/Audi_EN> <http://dbpedia.org/ontology/parentCompany> <http://dbpedia.org/resource/Volkswagen_EN> ."""
	)

	def dbpediaParsedRelations(): List[Relation] = List(
		Relation("Audi DE", "parentCompany", "Volkswagen DE")
	)

	def dbpediaRelations: List[Relation] = List(
		Relation("Audi", "parentCompany", "Volkswagen"),
		Relation("Audi", "subsidiary", "Lamborghini"),
		Relation("Audi", "type", "Aktiengesellschaft"),
		Relation("BMW", "division", "BMW Motorsport"),
		Relation("BWM", "foaf:Hompage", "http://www.bmw.com/"),
		Relation("BMW", "product", "Fahrrad")
	)

	def dbpedia: List[Subject] = List(
		Subject(id = idList.head, master= idList.head, datasource = datasource, name = Option("Audi")),
		Subject(id = idList(1), master= idList(1), datasource = datasource, name = Option("BMW")),
		Subject(id = idList(2), master= idList(2), datasource = datasource, name = Option("Volkswagen")),
		Subject(id = idList(3), master= idList(3), datasource = datasource, name = Option("BMW Motorsport")),
		Subject(id = idList(4), master= idList(4), datasource = datasource, name = Option("Commerzbank")),
		Subject(id = idList(5), master= idList(5), datasource = datasource, name = Option("Lamborghini"))
	)

	def dbpediaImportedRelations = List(
		Subject(
			id = idList.head,
			master = idList.head,
			datasource = datasource,
			name = Option("Audi"),
			relations = Map(
				idList(2) -> Map("parentCompany" -> ""),
				idList(5) -> Map("subsidiary" -> "")
			)
		),
		Subject(
			id = idList(1),
			master = idList(1),
			datasource = datasource,
			name = Option("BMW"),
			relations = Map(
				idList(3) -> Map("division" -> "")
			)
		)
	)
}
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
