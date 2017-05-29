package de.hpi.ingestion.datalake.models

import java.util.UUID

import org.scalatest.{FlatSpec, Matchers}

class SubjectTest extends FlatSpec with Matchers {
	"Equality" should "only compare UUIDs of Subjects" in {
		val uuid1 = UUID.randomUUID()
		val uuid2 = UUID.randomUUID()
		val sub1 = Subject(uuid1)
		val sub2 = Subject(uuid1)
		val sub3 = Subject(uuid2)
		sub1 shouldEqual sub2
		sub1 should not equal sub3
		sub1.equals(uuid1) shouldBe false
	}

	"Attribute values" should "be returned" in {
		val name = "name"
		val aliasList = List("alias")
		val sub1 = Subject(name = Option(name), aliases = aliasList)
		sub1.get("name") shouldEqual List(name)
		sub1.get("category") shouldBe empty
		sub1.get("aliases") shouldEqual aliasList
		sub1.get("id") shouldBe empty
		sub1.get("properties") shouldBe empty
	}
}
