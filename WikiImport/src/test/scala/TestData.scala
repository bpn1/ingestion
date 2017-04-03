import java.util.UUID
import DataLake.Subject
import scala.io.Source

object TestData {
	// scalastyle:off line.size.limit

	def unfilteredWikidataEntities(): List[WikiDataEntity] = {
		List(WikiDataEntity("Q5", label = Option("Mensch"), instancetype = Option("Don't resolve me")),
			WikiDataEntity("Q11570", label = Option("Kilogramm")),
			WikiDataEntity("Q9212", label = Option("United States Army")))
	}
	def filteredWikidataEntities(): List[WikiDataEntity] = {
		List(WikiDataEntity("Q11570", label = Option("Kilogramm")),
			WikiDataEntity("Q9212", label = Option("United States Army")))
	}

	def unresolvedWikidataEntities(): List[WikiDataEntity] = {
		List(WikiDataEntity("Q433465", label = Option("Jill Bakken"), data = Map("mass" -> List("+65;Q11570"), "instance of" -> List("Q5"), "military branch" -> List("Q9212"))))
	}

	def entityNameData(): List[(String, String)] = {
		List(("Q5", "Mensch"),
			("Q11570", "Kilogramm"),
			("Q9212", "United States Army"))
	}

	def flattenedWikidataEntries(): List[(String, String, String)] = {
		List(("Q433465", "mass", "+65;Q11570"),
			("Q433465", "instance of", "Q5"),
			("Q433465", "military branch", "Q9212"))
	}

	def wikidataIdEntries(): List[(String, String, String)] = {
		List(("Q433465", "instance of", "Q5"),
			("Q433465", "military branch", "Q9212"))
	}

	def resolvedWikidataIdEntries(): List[(String, String, String)] = {
		List(("Q433465", "instance of", "Mensch"),
			("Q433465", "military branch", "United States Army"))
	}

	def unitWikidataIdEntries(): List[(String, String, String)] = {
		List(("Q433465", "mass", "+65;Q11570"))
	}

	def splitUnitWikidataIdEntries(): List[(String, (String, String, String))] = {
		List(("Q11570", ("Q433465", "mass", "+65")))
	}

	def resolvedUnitWikidataIdEntries(): List[(String, String, String)] = {
		List(("Q433465", "mass", "+65;Kilogramm"))
	}

	def testSubjects(): List[Subject] = {
		List(Subject(UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58"), name = Option("Entry 1"), properties = Map(FindRelations.wikiDataIdKey -> List("Q1"), "test" -> List("Q2"))),
			Subject(UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f"), name = Option("Entry 2"), properties = Map(FindRelations.wikiDataIdKey -> List("Q2"), "test" -> List("Q3"))),
			Subject(UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d"), name = Option("Entry 3"), properties = Map(FindRelations.wikiDataIdKey -> List("Q3"), "test" -> List("Q4"))),
			Subject(UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"), name = Option("Entry 4"), properties = Map(FindRelations.wikiDataIdKey -> List("Q4"))),
			Subject(UUID.fromString("7bfd2ffe-154b-486a-b30d-581d785940c6"), name = Option("Entry 5"), properties = Map(FindRelations.wikiDataIdKey -> List[String](), "test" -> List("Q1"))),
			Subject(UUID.fromString("bbd8f942-1663-4fea-9e70-3cf27896bc57"), name = Option("Entry 6"), properties = Map("test" -> List("Q5"))))
	}

	def resolvedNameMap(): Map[String, (UUID, String)] = {
		Map("Q1" -> (UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58"), "Entry 1"),
			"Q2" -> (UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f"), "Entry 2"),
			"Q3" -> (UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d"), "Entry 3"),
			"Q4" -> (UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"), "Entry 4"))
	}

	def resolvedTestSubjects(): List[Subject] = {
		List(Subject(UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58"), name = Option("Entry 1"), properties = Map(FindRelations.wikiDataIdKey -> List("Q1"), "test" -> List("Q2", "Entry 2")), relations = Map(UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f") -> Map("type" -> "test"))),
			Subject(UUID.fromString("7f6891ef-c72a-4af5-a4af-f134f846413f"), name = Option("Entry 2"), properties = Map(FindRelations.wikiDataIdKey -> List("Q2"), "test" -> List("Q3", "Entry 3")), relations = Map(UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d") -> Map("type" -> "test"))),
			Subject(UUID.fromString("5788c6f5-9696-4d67-a592-fd9b8c4e5a9d"), name = Option("Entry 3"), properties = Map(FindRelations.wikiDataIdKey -> List("Q3"), "test" -> List("Q4", "Entry 4")), relations = Map(UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d") -> Map("type" -> "test"))),
			Subject(UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"), name = Option("Entry 4"), properties = Map(FindRelations.wikiDataIdKey -> List("Q4"))),
			Subject(UUID.fromString("7bfd2ffe-154b-486a-b30d-581d785940c6"), name = Option("Entry 5"), properties = Map(FindRelations.wikiDataIdKey -> List[String](), "test" -> List("Q1", "Entry 1")), relations = Map(UUID.fromString("41e7b945-0f73-430a-be7c-580fc7a09f58") -> Map("type" -> "test"))),
			Subject(UUID.fromString("bbd8f942-1663-4fea-9e70-3cf27896bc57"), name = Option("Entry 6"), properties = Map("test" -> List("Q5"))))
	}

	def testWikidataEntities(): List[WikiDataEntity] = {
		List(WikiDataEntity("Q21110253", List("testalias"), Option("human protein (annotated by UniProtKB/Swiss-Prot Q8N128)"), Option("item"), Option("testwikiname"), Option("en_testwikiname"), Option("test_instancetype"), Option("Protein FAM177A1"), Map("Ensembl Protein ID" -> List("ENSP00000280987", "ENSP00000371843", "ENSP00000379734"), "subclass of" -> List("Protein", "FAM177 family"))))
	}

	def translatedSubjects(): List[Subject] = {
		List(Subject(
			name = Option("Protein FAM177A1"),
			aliases = List("testalias"),
			category = Option("test_instancetype"),
			properties = Map(
				"wikipedia_name" -> List("testwikiname"),
				"Ensembl Protein ID" -> List("ENSP00000280987", "ENSP00000371843", "ENSP00000379734"),
				"subclass of" -> List("Protein", "FAM177 family"),
				"wikidata_id" -> List("Q21110253"))))
	}

	def classWikidataEntities(): List[WikiDataEntity] = {
		List(
			WikiDataEntity("Q1", label = Option("Entry 1"), data = Map("instance of" -> List("Q2"), "subclass of" -> List("Q3"), "test" -> List("testVal"))),
			WikiDataEntity("Q2", label = Option("Entry 2"), data = Map("instance of" -> List("Q1"), "test" -> List("testVal"))),
			WikiDataEntity("Q3", label = Option("Entry 3"), data = Map("subclass of" -> List("Q4"))),
			WikiDataEntity("Q4", label = Option("Entry 4"), data = Map("test" -> List("testVal"))))
	}

	def subclassEntries(): List[SubclassEntry] = {
		List(
			SubclassEntry("Q1", "Entry 1", Map("instance of" -> List("Q2"), "subclass of" -> List("Q3"))),
			SubclassEntry("Q2", "Entry 2", Map("instance of" -> List("Q1"))),
			SubclassEntry("Q3", "Entry 3", Map("subclass of" -> List("Q4"))),
			SubclassEntry("Q4", "Entry 4"))
	}

	def subclassOfProperties(): List[SubclassEntry] = {
		List(
			SubclassEntry("Q1", "Entry 1", Map("instance of" -> List("Q2"), "subclass of" -> List("Q3"))),
			SubclassEntry("Q3", "Entry 3", Map("subclass of" -> List("Q4"))))
	}

	def oldClassMap(): Map[String, List[String]] = {
		Map(
			"Q3" -> List("Entry 4", "Entry 5", "Entry 3"),
			"Q4" -> List("Entry 4"))
	}

	def newClassMap(): Map[String, List[String]] = {
		Map(
			"Q1" -> List("Entry 4", "Entry 3", "Entry 1"),
			"Q3" -> List("Entry 4", "Entry 3"),
			"Q4" -> List("Entry 4", "Entry 4", "Entry 3"))
	}

	def classMap(): Map[String, List[String]] = {
		Map(
			"Q1" -> List("Entry 4", "Entry 3", "Entry 1"),
			"Q3" -> List("Entry 4", "Entry 3"),
			"Q4" -> List("Entry 4"))
	}

	def validInstanceOfProperties(): List[SubclassEntry] = {
		List(SubclassEntry("Q2", "Entry 2", Map("instance of" -> List("Q1"))))
	}

	def updatedInstanceOfProperties(): List[(String, String, Map[String, List[String]])] = {
		List(("Q2", "Entry 4", Map("wikidata_path" -> List("Entry 4", "Entry 3", "Entry 1"))))
	}

	def classesToTag(): Map[String, String] = {
		Map("Q4" -> "Entry 4")
	}

	def rawWikiDataEntries(): List[String] = {
		Source.fromURL(getClass.getResource("/wikidataTestData"))
			.getLines()
			.toList
	}

	def dataTypeValues(dataType: String): String = {
		dataType match {
			case "string" => "test string 1"
			case "wikibase-entityid" => "test id 1"
			case "time" => "test time 1"
			case "monolingualtext" => "test text 1"
			case "globecoordinate" => "1.0;2.0"
			case "quantity" => "test amount;test unit"
		}
	}

	// scalastyle:on line.size.limit
}
