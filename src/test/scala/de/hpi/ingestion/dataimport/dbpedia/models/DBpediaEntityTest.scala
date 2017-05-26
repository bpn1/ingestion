package de.hpi.ingestion.dataimport.dbpedia.models

import org.scalatest.{FlatSpec, Matchers}

class DBpediaEntityTest extends FlatSpec with Matchers {
	"Attribute values" should "be returned" in {
		val id = "Q1"
		val name = "name"
		val sub1 = DBpediaEntity(dbpedianame = id, label = Option(name))
		sub1.get("dbpedianame") shouldBe List(id)
		sub1.get("label") shouldEqual List(name)
		sub1.get("description") shouldBe empty
		sub1.get("data") shouldBe empty
		sub1.get("category") shouldBe empty
	}
}
