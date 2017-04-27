package de.hpi.ingestion.implicits

object TestData {

	def crossableLists: (List[String], List[String]) = {
		(List("a1", "a2"), List("b1", "b2", "b3"))
	}

	def crossProduct: Set[(String, String)] = {
		Set(
			("a1", "b1"),
			("a1", "b2"),
			("a1", "b3"),
			("a2", "b1"),
			("a2", "b2"),
			("a2", "b3")
		)
	}
}
