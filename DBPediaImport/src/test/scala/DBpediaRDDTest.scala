import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source

class DBpediaRDDTest extends FlatSpec with SharedSparkContext with RDDComparisons {

	"Triples" should "be tokenized into three elements" in {
		turtleRDD()
		    .map(DBPediaImport.tokenize)
		    .collect
		    .foreach(tripleList => assert(tripleList.size == 3))
	}

	they should "have namespace prefixes after cleaning" in {
		val parsed = turtleRDD()
			.map(DBPediaImport.tokenize)
			.collect
		    .map(tripleList => tripleList.map(el => DBPediaImport.cleanURL(el, prefixesList())))
			.map { case List(a, b, c) => (a, b, c) }
		    .toList
		val triples = tripleRDD()
			.map(el => (el._1, el._2._1, el._2._2))
			.collect
			.toList
		assert(parsed == triples)
	}

	"DBpediaEntities" should "not be empty" in {
		val rdd = tripleRDD().groupByKey
			.map(tuple => DBPediaImport.extractProperties(tuple._1, tuple._2.toList))
		assert(rdd.count > 0)
	}

	they should "be instances of DBpediaEntitiy" in {
		tripleRDD().groupByKey
			.map(tuple => DBPediaImport.extractProperties(tuple._1, tuple._2.toList))
		    .collect
		    .foreach(entity => assert(entity.isInstanceOf[DBPediaEntity]))
	}

	they should "contain the same information as the triples" in {
		val entities = tripleRDD()
			.groupByKey
			.map(tuple => DBPediaImport.extractProperties(tuple._1, tuple._2.toList))
		assertRDDEquals(entityRDD(), entities)
	}

	def turtleRDD(): RDD[String] = {
		sc.parallelize(List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/V> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Autor> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/V> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Wikipedia:Liste> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/T> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Autor> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/T> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Wikipedia:Liste> ."""
		))
	}

	def prefixesList(): List[(String,String)] = {
		val prefixFile = Source.fromURL(getClass.getResource("/prefixes.txt"))
		val prefixes = prefixFile.getLines.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))
		prefixes
	}

	def tripleRDD(): RDD[(String, (String, String))] = {
		sc.parallelize(List(
			("dbpedia-de:Anschluss_(Soziologie)", ("dct:subject", "dbpedia-de:Kategorie:Soziologische_Systemtheorie")),
			("dbpedia-de:Liste_von_Autoren/V", ("dct:subject", "dbpedia-de:Kategorie:Autor")),
			("dbpedia-de:Liste_von_Autoren/V", ("dct:subject", "dbpedia-de:Kategorie:Wikipedia:Liste")),
			("dbpedia-de:Liste_von_Autoren/T", ("dct:subject", "dbpedia-de:Kategorie:Autor")),
			("dbpedia-de:Liste_von_Autoren/T", ("dct:subject", "dbpedia-de:Kategorie:Wikipedia:Liste"))
		))
	}

	def entityRDD(): RDD[DBPediaEntity] = {
		sc.parallelize(List(
			DBPediaEntity(dbpedianame="dbpedia-de:Liste_von_Autoren/V", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste"))),
			DBPediaEntity(dbpedianame="dbpedia-de:Anschluss_(Soziologie)", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Soziologische_Systemtheorie"))),
			DBPediaEntity(dbpedianame="dbpedia-de:Liste_von_Autoren/T", data=Map("dct:subject" -> List("dbpedia-de:Kategorie:Autor", "dbpedia-de:Kategorie:Wikipedia:Liste")))
		))
	}
}
