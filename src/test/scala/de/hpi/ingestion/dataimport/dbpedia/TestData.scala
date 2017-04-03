package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity
import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.Source

object TestData {
	// scalastyle:off line.size.limit

	def prefixesList(): List[(String,String)] = {
		val prefixFile = Source.fromURL(getClass.getResource("/prefixes.txt"))
		val prefixes = prefixFile.getLines.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))
		prefixes
	}

	def line(): String = {
		"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> ."""
	}

	def shorterLineList(): List[String] = {
		List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> .""",
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> .""",
			"")
	}

	def longerLineList(): List[String] = {
		List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> .""",
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://purl.org/dc/terms/subject> ."""
		)
	}

	def lineTokens(): List[String] =  {
		List(
			"http://de.dbpedia.org/resource/Anschluss_(Soziologie)",
			"http://purl.org/dc/terms/subject",
			"http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie"
		)
	}

	def properties(): List[(String, String)] =  {
		List(
			("dbo:wikiPageID", "1"),
			("rdfs:label", "Anschluss"),
			("dbo:abstract", "Der Anschluss ist der Anschluss zum Anschluss"),
			("rdf:type", "Typ 0"),
			("ist", "klein"),
			("ist", "mittel"),
			("hat", "Namen"),
			("hat", "Nachnamen"),
			("kennt", "alle")
		)
	}

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
			DBPediaEntity(dbpedianame="dbpedia-de:Liste_von_Autoren/V", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste"))),
			DBPediaEntity(dbpedianame="dbpedia-de:Anschluss_(Soziologie)", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Soziologische_Systemtheorie"))),
			DBPediaEntity(dbpedianame="dbpedia-de:Liste_von_Autoren/T", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste")))
		))
	}

	def dbpediaSubjectList(): List[Subject] = {
		List(
			Subject(name=Some("Liste_von_Autoren/V@de ."), properties = Map("owl:sameAs" -> List("dbpedia:Liste von Autoren", "yago:Liste von Autoren"))),
			Subject(name=Some("Ausschluss@de ."), properties = Map("owl:sameAs" -> List("wikidata:Q123456789", "yago:Q123456789"))),
			Subject(name=Some("Liste_von_Autoren/T@de ."), properties = Map("owl:sameAs" -> List("wikidata:Q54321", "yago:Q54321")))
		)
	}

	def subjectList(): List[Subject] = {
		List(
			Subject(name=Some("Liste_von_Autoren_5"), properties = Map("wikidata_id" -> List("Q123456789"), "wikipedia_name" -> List("Liste_von_Autoren/V"))),
			Subject(name=Some("Anschluss_(Soziologie)"), properties = Map("wikidata_id" -> List("Q54321"), "wikipedia_name" -> List("Liste_von_Autoren/U"))),
			Subject(name=Some("Liste_von_Autoren_T"), properties = Map("wikidata_id" -> List("Q12345"), "wikipedia_name" -> List("Liste_von_Autoren/T")))
		)
	}

	def singleKeyRDD(sc: SparkContext, subjects: List[Subject]): RDD[(String, Subject)] = {
		sc.parallelize(Seq(
			("Liste_von_Autoren/V", subjects.head),
			("Liste_von_Autoren/U", subjects(1)),
			("Liste_von_Autoren/T", subjects(2))
		))
	}

	def multiKeyRDD(sc: SparkContext, subjects: List[Subject]): RDD[(List[String], Subject)] = {
		sc.parallelize(Seq(
			(List("dbpedia:Liste von Autoren", "yago:Liste von Autoren"), subjects.head),
			(List("wikidata:Q123456789", "yago:Q123456789"), subjects(1)),
			(List("wikidata:Q54321", "yago:Q54321"), subjects(2))
		))
	}

	def nameJoinedRDD(sc: SparkContext, dbpedia: List[Subject], subjects: List[Subject]): RDD[(String, (Subject, Subject))] = {
		sc.parallelize(Seq(
			("Liste_von_Autoren/V", (dbpedia.head, subjects.head)),
			("Liste_von_Autoren/T", (dbpedia(2), subjects(2)))
		))
	}

	def idJoinedRDD(sc: SparkContext, dbpedia: List[Subject], subjects: List[Subject]): RDD[(String, (Subject, Subject))] = {
		sc.parallelize(Seq(
			("Q123456789", (dbpedia(1), subjects.head)),
			("Q54321", (dbpedia(2), subjects(1)))
		))
	}

	// scalastyle:on line.size.limit
}
