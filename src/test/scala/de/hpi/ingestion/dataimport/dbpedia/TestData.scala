package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.dataimport.dbpedia.DBPediaDeduplication.DuplicateCandidate
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
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
			Subject(name=Option("Liste_von_Autoren/V"), properties = Map("wikidata_id" -> List("Q123456789"))),
			Subject(name=Option("Ausschluss"), properties = Map("owl:sameAs" -> List("dbpedia:Liste von Autoren", "yago:Liste von Autoren"))),
			Subject(name=Option("Liste_von_Autoren/T"), properties = Map("wikidata_id" -> List("Q54321")))
		)
	}

	def subjectList(): List[Subject] = {
		List(
			Subject(name=Option("Liste_von_Autoren_5"), properties = Map("wikidata_id" -> List("Q123456789"), "wikipedia_name" -> List("Liste_von_Autoren/V"))),
			Subject(name=Option("Anschluss_(Soziologie)"), properties = Map("wikidata_id" -> List("Q54321"), "wikipedia_name" -> List("Liste_von_Autoren/U"))),
			Subject(name=Option("Liste_von_Autoren_T"), properties = Map("wikidata_id" -> List("Q12345"), "wikipedia_name" -> List("Liste_von_Autoren/T")))
		)
	}

	def subjectDuplicatesRDD(sc: SparkContext, dbpedia: List[Subject], subjects: List[Subject]): RDD[(Subject, List[Subject])] = {
		sc.parallelize(Seq(
			(subjects.head, List(dbpedia.head)),
			(subjects(1), List(dbpedia(2))),
			(subjects(2), List(dbpedia(2)))
		))
	}

	val mergedSubject = Subject(
		name=Option("Liste_von_Autoren_5"),
		properties=Map(
			"wikidata.wikidata_id" -> List("Q123456789"),
			"wikidata.wikipedia_name" -> List("Liste_von_Autoren/V"),
			"dbpedia.wikidata_id" -> List("Q123456789"),
			"dbpedia.name" -> List("Liste_von_Autoren/V")
		)
	)

	def joinedSubjectRDD(sc: SparkContext, dbpedia: List[Subject], subjects: List[Subject]): RDD[Subject] = {
		sc.parallelize(Seq(
			dbpedia.head,
			dbpedia(2),
			subjects.head,
			subjects(1),
			subjects(2)
		))
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
			(List("dbpedia:Liste von Autoren", "yago:Liste von Autoren"), subjects(1))
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
			("Q123456789", (dbpedia.head, subjects.head)),
			("Q54321", (dbpedia(2), subjects(1)))
		))
	}

	def nameCandidates(sc: SparkContext, dbpedia: List[Subject], subjects: List[Subject]): RDD[DuplicateCandidate] = {
		sc.parallelize(Seq(
			DuplicateCandidate(subjects.head.id, dbpedia.head, "Test"),
			DuplicateCandidate(subjects(2).id, duplicate = dbpedia(2), duplicate_table = "Test")
		))
	}

	def idCandidates(sc: SparkContext, dbpedia: List[Subject], subjects: List[Subject]): RDD[DuplicateCandidate] = {
		sc.parallelize(Seq(
			DuplicateCandidate(subjects.head.id, dbpedia.head, "Test"),
			DuplicateCandidate(subjects(1).id, dbpedia(2), "Test")
		))
	}

	def candidatesJoinedRDD(sc: SparkContext, dbpedia: List[Subject], subjects: List[Subject]): RDD[DuplicateCandidates] = {
		sc.parallelize(Seq(
			DuplicateCandidates(subjects.head.id, List((dbpedia.head, "Test", 1.0))),
			DuplicateCandidates(subjects(1).id, List((dbpedia(2), "Test", 1.0))),
			DuplicateCandidates(subjects(2).id, List((dbpedia(2), "Test", 1.0)))
		))
	}

	// scalastyle:on line.size.limit
}
