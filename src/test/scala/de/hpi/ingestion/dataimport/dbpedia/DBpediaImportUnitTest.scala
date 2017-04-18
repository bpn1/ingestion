package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity
import org.scalatest.FlatSpec

class DBpediaImportUnitTest extends FlatSpec with SharedSparkContext {

	"tokenize" should "return a three element long list" in {
		assert(DBPediaImport.tokenize(TestData.line()).length == 3)
	}

	it should "tokenize the triple correctly" in {
		val parsedTokens = DBPediaImport.tokenize(TestData.line())
		assert(parsedTokens == TestData.lineTokens())
	}

	it should "return a three element long list with a shorter input" in {
		TestData.shorterLineList().foreach(line =>
			assert(DBPediaImport.tokenize(line).length == 3))
	}

	it should "return a three element long list with a longer input" in {
		TestData.longerLineList().foreach(line =>
			assert(DBPediaImport.tokenize(line).length == 3))
	}

	"cleanURL" should "replace all prefixes" in {
		val cleanList = TestData.lineTokens()
			.map(el => DBPediaImport.cleanURL(el, TestData.prefixesList()))
		assert(cleanList.head.startsWith("dbpedia-de:"))
		assert(cleanList(1).startsWith("dct:"))
		assert(cleanList(2).startsWith("dbpedia-de:"))
	}

	"extractProperties" should "create a DBPediaEntity from properties" in {
		val entity = DBPediaImport.extractProperties("Test Objekt", TestData.properties())
		val expectedEntity = DBPediaEntity(
			"Test Objekt",
			Some("1"),
			Some("Anschluss"),
			Some("Der Anschluss ist der Anschluss zum Anschluss"),
			Some("Typ 0"),
			Map(
				"ist" -> List("klein", "mittel"),
				"hat" -> List("Namen", "Nachnamen"),
				"kennt" -> List("alle")
			)
		)
		assert(expectedEntity === entity)
	}
}
