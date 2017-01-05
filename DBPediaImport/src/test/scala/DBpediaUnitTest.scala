import org.scalatest.FlatSpec

class DBpediaUnitTest extends FlatSpec {

	"parseLine" should "return a DBpediaTriple" in {
		val triple = DBPediaImport.parseLine(line)
		assert(triple.isInstanceOf[DBPediaImport.DBPediaTriple])
	}

	it should "tokenize the triple correctly" in {
		val tokens = DBPediaImport.tokenize(line)
		assert(tokens.length == 3)
	}

	"extractProperties" should "return a List of Tuples" in {
		val properties = DBPediaImport.extractProperties(group)
		assert(properties.isInstanceOf[List[Tuple2[String, String]]])
	}

	it should "contain the subject as property" in {
		val properties = DBPediaImport.extractProperties(group)
		assert(properties.exists(tuple => tuple._1 == "dbpedia-entity" && tuple._2 == group._1))
	}

	"createMap" should "return a not empty Map" in {
		val map = DBPediaImport.createMap(properties)
		assert(map.isInstanceOf[Map[String, List[String]]])
		assert(map.nonEmpty)
	}

	it should "contain a key->value pair for each unique predicate" in {
		val map = DBPediaImport.createMap(properties)
		assert(map.size == 3)
	}

	"translateToDBpediaEntity" should "return a DBpediaEntity" in {
		val entity = DBPediaImport.translateToDBPediaEntry(map)
		assert(entity.isInstanceOf[DBPediaEntity])
	}

	it should "have a wikipageID" in {
		val entity = DBPediaImport.translateToDBPediaEntry(map)
		assert(entity.wikipageId.nonEmpty)
	}

	it should "have a dbpediaName" in {
		val entity = DBPediaImport.translateToDBPediaEntry(map)
		assert(entity.dbPediaName.nonEmpty)
	}

	it should "have a label optionally" in {
		val entity = DBPediaImport.translateToDBPediaEntry(map)
		val entityWithoutLabel = DBPediaImport.translateToDBPediaEntry(mapWithoutLabelAndDescription)
		assert(entity.label.isDefined)
		assert(entityWithoutLabel.label.isEmpty)
	}

	it should "have a description optionally" in {
		val entity = DBPediaImport.translateToDBPediaEntry(map)
		val entityWithoutDescription = DBPediaImport.translateToDBPediaEntry(mapWithoutLabelAndDescription)
		assert(entity.description.isDefined)
		assert(entityWithoutDescription.description.isEmpty)
	}

	it should "have a data Map for other properties" in {
		val entity = DBPediaImport.translateToDBPediaEntry(map)
		assert(entity.data.size == 3)
	}

	val line = """<http://de.dbpedia.org/resource/Anschluss_(Soziologie)> <http://purl.org/dc/terms/subject> <http://de.dbpedia.org/resource/Kategorie:Soziologische_Systemtheorie> ."""

	val group = Tuple2(
		"dbpedia-db:Anschluss_(Soziologie)",
		List(DBPediaImport.DBPediaTriple("dbpedia-db:Anschluss_(Soziologie)","ist","klein"), DBPediaImport.DBPediaTriple("dbpedia-db:Anschluss_(Soziologie)", "ist", "mittel"), DBPediaImport.DBPediaTriple("dbpedia-db:Anschluss_(Soziologie)", "ist", "groß"))
	)

	val properties = List(Tuple2("ist", "klein"), Tuple2("ist", "mittel"), Tuple2("hat", "Namen"), Tuple2("hat", "Nachnamen"), Tuple2("kennt", "alle"))

	val map = Map(
		"dbpedia-entity" -> List("dbpedia-db:Anschluss_(Soziologie)"),
		"dbo:wikiPageID" -> List("1"),
		"rdfs:label" -> List("Anschluss"),
		"dbo:abstract" -> List("Der Anschluss ist der Anschluss zum Anschluss"),
		"ist" -> List("klein", "mittel", "groß"),
		"hat" -> List("Namen", "Nachnamen"),
		"kennt" -> List("alle")
	)

	val mapWithoutLabelAndDescription = Map(
		"dbpedia-entity" -> List("dbpedia-db:Anschluss_(Soziologie)"),
		"dbo:wikiPageID" -> List("1")
	)
}
