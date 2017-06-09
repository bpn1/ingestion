package de.hpi.ingestion.dataimport.wikidata.models

import org.scalatest.{FlatSpec, Matchers}

class WikiDataEntityTest extends FlatSpec with Matchers {
	"Attribute values" should "be returned" in {
		val id = "Q1"
		val name = "name"
		val aliasList = List("alias")
		val sub1 = WikiDataEntity(id = id, label = Option(name), aliases = aliasList)
		sub1.get("id") shouldBe List(id)
		sub1.get("label") shouldEqual List(name)
		sub1.get("description") shouldBe empty
		sub1.get("aliases") shouldEqual aliasList
		sub1.get("data") shouldBe empty
		sub1.get("category") shouldBe empty
	}
}
