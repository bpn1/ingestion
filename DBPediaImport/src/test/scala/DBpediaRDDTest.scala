import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

class DBpediaRDDTest extends FlatSpec with SharedSparkContext {

	"DBpediaTriple store" should "not be empty" in {
		val rdd = DBPediaImport.parseTurtleFile(TurtleRDD())
		assert(rdd.count > 0)
	}

	they should "be instance of DBpediaTriple" in {
		val rdd = DBPediaImport.parseTurtleFile(TurtleRDD())
		assert(rdd.isInstanceOf[RDD[DBPediaImport.DBPediaTriple]])
	}

	they should "have namespaces prefixes" in {
		val ttl = DBPediaImport.parseTurtleFile(TurtleRDD()).collect()
		val triple = DBpediaTripleRDD().collect()
	}

	"DBpediaEntities" should "not be empty" in {
		val rdd = DBPediaImport.createDBpediaEntities(DBpediaTripleRDD())
		assert(rdd.count > 0)
	}

	they should "be instance of DBpediaEntities" in {
		val rdd = DBPediaImport.createDBpediaEntities(DBpediaTripleRDD())
		assert(rdd.isInstanceOf[RDD[DBPediaEntity]])
	}

	def TurtleRDD(): RDD[String] = {
		sc.parallelize(List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/V> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Autor> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/V> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Wikipedia:Liste> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/T> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Autor> .""",
			"""<http://de.dbpedia.org/resource/Liste_von_Autoren/T> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Wikipedia:Liste> ."""
		))
	}

	def DBpediaTripleRDD(): RDD[DBPediaImport.DBPediaTriple] = {
		sc.parallelize(List(
			DBPediaImport.DBPediaTriple("dbr:Anschluss_(Soziologie)","dc:subject","dbr:Kategorie:Soziologische_Systemtheorie"),
			DBPediaImport.DBPediaTriple("dbr:Liste_von_Autoren/V","dc:subject","dbr:Kategorie:Autor"),
			DBPediaImport.DBPediaTriple("dbr:Liste_von_Autoren/V","dc:subject","dbr:Kategorie:Wikipedia:Liste"),
			DBPediaImport.DBPediaTriple("dbr:Liste_von_Autoren/T","dc:subject","dbr:Kategorie:Autor"),
			DBPediaImport.DBPediaTriple("dbr:Liste_von_Autoren/T","dc:subject","dbr:Kategorie:Wikipedia:Liste")
		))
	}
}
