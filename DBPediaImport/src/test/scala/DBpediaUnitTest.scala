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
