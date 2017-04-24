package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class DBPediaImportUnitTest extends FlatSpec with SharedSparkContext with Matchers {

	"tokenize" should "return a three element long list" in {
		val tokens = DBPediaImport.tokenize(TestData.line)
		tokens should have length 3
	}

	it should "return a three element long list with a shorter input" in {
		TestData.shorterLineList.foreach{ line =>
			val tokens = DBPediaImport.tokenize(line)
			tokens should have length 3
		}
	}

	it should "return a three element long list with a longer input" in {
		TestData.longerLineList.foreach{ line =>
			val tokens = DBPediaImport.tokenize(line)
			tokens should have length 3
		}
	}

	it should "tokenize the triple correctly" in {
		val parsedTokens = DBPediaImport.tokenize(TestData.line)
		val expected = TestData.lineTokens
		parsedTokens shouldEqual expected
	}

	"cleanURL" should "replace all prefixes" in {
		val tokens = TestData.lineTokens
		val prefixList = TestData.prefixesList
		val cleanList = tokens.map(DBPediaImport.cleanURL(_, prefixList))
		cleanList.head should startWith ("dbpedia-de:")
		cleanList(1) should startWith ("dct:")
		cleanList(2) should startWith ("dbpedia-de:")
	}

	"extractInstancetype" should "extract the right type from a list" in {
		val rdfTypes = List("owl:Thing", "dbo:Agent", "dbo:Organisation", "dbo:Company")
		val organisations = TestData.organisations
		val instanceType = DBPediaImport.extractInstancetype(rdfTypes, organisations)
		val expected = Option("Company")
		instanceType shouldEqual expected
	}

	it should "return None if given list is empty" in {
		val organisations = TestData.organisations
		val instanceType = DBPediaImport.extractInstancetype(Nil, organisations)
		instanceType shouldEqual None
	}

	it should "return None if no sublass of organisation could be found" in {
		val rdfTypes = List("owl:Thing", "dbo:Agent", "dbo:Family")
		val organisations = TestData.organisations
		val instanceType = DBPediaImport.extractInstancetype(rdfTypes, organisations)
		instanceType shouldEqual None
	}

	"extractProperties" should "create a DBPediaEntity from properties" in {
		val name = "Test Entity"
		val entity = DBPediaImport.extractProperties(name, TestData.properties, TestData.organisations)
		val expected = TestData.parsedEntity(name)
		entity shouldEqual expected
	}
}
