import org.apache.spark.rdd.RDD

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

	def resolvedWikidataEntities(): List[WikiDataEntity] = {
		List(WikiDataEntity("Q433465", label = Option("Jill Bakken"), data = Map("mass" -> List("+65;Kilogramm"), "instance of" -> List("Mensch"), "military branch" -> List("Q9212"))))
	}

	def joinedIds(): List[(String, String, String)] = {
		List()
	}

	def testResolveNameMap(): Map[String, String] = {
		Map("Q5" -> "Mensch",
			"Q11570" -> "Kilogramm",
			"Q9212" -> "United States Army")
	}

	// scalastyle:on line.size.limit
}
