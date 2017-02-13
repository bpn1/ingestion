import org.apache.spark.broadcast.Broadcast
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec
import scala.io.Source

class DBpediaUnitTest extends FlatSpec with SharedSparkContext {

	"tokenize" should "return a three element long list" in {
		assert(DBPediaImport.tokenize(line).length == 3)
	}

	it should "tokenize the triple correctly" in {
		val parsedTokens = DBPediaImport.tokenize(line)
		assert(parsedTokens == lineTokens)
	}

	it should "return a three element long list with a shorter input" in {
		val lineList = List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> .""",
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> .""",
			"")
		lineList.foreach(line => assert(DBPediaImport.tokenize(line).length == 3))
	}

	it should "return a three element long list with a longer input" in {
		val lineList = List(
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> .""",
			"""<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://purl.org/dc/terms/subject> ."""
		)
		lineList.foreach(line => assert(DBPediaImport.tokenize(line).length == 3))
	}

	"cleanURL" should "replace all prefixes" in {
		val cleanList = lineTokens.map(el => DBPediaImport.cleanURL(el, prefixesList))
		assert(cleanList.head.startsWith("dbpedia-de:"))
		assert(cleanList(1).startsWith("dct:"))
		assert(cleanList(2).startsWith("dbpedia-de:"))
	}

	// TODO test extractProperties()

//	it should "contain the subject as property" in {
//		val properties = DBPediaImport.extractProperties(group)
//		assert(properties.exists(tuple => tuple._1 == "dbpedia-entity" && tuple._2 == group._1))
//	}
//
//	"createMap" should "return a not empty Map" in {
//		val map = DBPediaImport.createMap(properties)
//		assert(map.isInstanceOf[Map[String, List[String]]])
//		assert(map.nonEmpty)
//	}
//
//	it should "contain a key->value pair for each unique predicate" in {
//		val map = DBPediaImport.createMap(properties)
//		assert(map.size == 3)
//	}
//
//	"translateToDBpediaEntity" should "return a DBpediaEntity" in {
//		val entity = DBPediaImport.translateToDBPediaEntry(map)
//		assert(entity.isInstanceOf[DBPediaEntity])
//	}
//
//	it should "have a wikipageId" in {
//		val entity = DBPediaImport.translateToDBPediaEntry(map)
//		assert(entity.wikipageid.nonEmpty)
//	}
//
//	it should "have a dbPediaName" in {
//		val entity = DBPediaImport.translateToDBPediaEntry(map)
//		assert(entity.dbpedianame.nonEmpty)
//	}
//
//	it should "have a label optionally" in {
//		val entity = DBPediaImport.translateToDBPediaEntry(map)
//		val entityWithoutLabel = DBPediaImport.translateToDBPediaEntry(mapWithoutLabelAndDescription)
//		assert(entity.label.isDefined)
//		assert(entityWithoutLabel.label.isEmpty)
//	}
//
//	it should "have a description optionally" in {
//		val entity = DBPediaImport.translateToDBPediaEntry(map)
//		val entityWithoutDescription = DBPediaImport.translateToDBPediaEntry(mapWithoutLabelAndDescription)
//		assert(entity.description.isDefined)
//		assert(entityWithoutDescription.description.isEmpty)
//	}
//
//	it should "have a data Map for other properties" in {
//		val entity = DBPediaImport.translateToDBPediaEntry(map)
//		assert(entity.data.size == 3)
//	}

	def prefixesList(): List[(String,String)] = {
		val prefixFile = Source.fromURL(getClass.getResource("/prefixes.txt"))
		val prefixes = prefixFile.getLines.toList
			.map(_.trim.replaceAll("""[()]""", "").split(","))
			.map(pair => (pair(0), pair(1)))
		prefixes
	}

	val line = """<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> ."""

	val lineTokens = List(
		"http://de.dbpedia.org/resource/Anschluss_(Soziologie)",
		"http://purl.org/dc/terms/subject",
		"http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie")

	val propTuple = ("dbpedia-de:Anschluss_(Soziologie)", List(("dbpedia-db:Anschluss_(Soziologie)","ist","klein"), ("dbpedia-db:Anschluss_(Soziologie)", "ist", "mittel"), ("dbpedia-db:Anschluss_(Soziologie)", "ist", "groß")))

	val properties = List(Tuple2("ist", "klein"), Tuple2("ist", "mittel"), Tuple2("hat", "Namen"), Tuple2("hat", "Nachnamen"), Tuple2("kennt", "alle"))

	val map = Map(
		"dbpedia-entity" -> List("dbpedia-de:Anschluss_(Soziologie)"),
		"dbo:wikiPageID" -> List("1"),
		"rdfs:label" -> List("Anschluss"),
		"dbo:abstract" -> List("Der Anschluss ist der Anschluss zum Anschluss"),
		"ist" -> List("klein", "mittel", "groß"),
		"hat" -> List("Namen", "Nachnamen"),
		"kennt" -> List("alle")
	)

	val mapWithoutLabelAndDescription = Map(
		"dbpedia-entity" -> List("dbpedia-de:Anschluss_(Soziologie)"),
		"dbo:wikiPageID" -> List("1")
	)
}
