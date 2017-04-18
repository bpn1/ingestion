package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import play.api.libs.json.{JsObject, JsValue, Json}

class WikiDataImportTest extends FlatSpec with SharedSparkContext with Matchers {
	"Json" should "be cleaned and valid json" in {
		// invalid json would cause an exception
		TestData.rawWikiDataEntries()
			.map(WikiDataImport.cleanJSON)
		    .filter(_.nonEmpty)
			.map(Json.parse)
	}

	"Data types" should "be parsed" in {
		val claimJson = Json.parse(TestData.claimData()).as[JsObject]
		claimJson.value.foreach { case (dataType, jsonData) =>
			val extractedData = WikiDataImport.parseDataType(Option(dataType), jsonData)
			val expectedValue = TestData.dataTypeValues(dataType)
			extractedData shouldEqual expectedValue
		}
	}

	"Labels" should "be extracted" in {
		val testEntities = Json.parse(TestData.rawWikidataEntries()).as[List[JsValue]]
			.map(value => WikiDataImport.extractLabels(value, WikiDataImport.extractString(value, List("type"))))
		val expectedLabels = TestData.entityLabels()
		testEntities shouldEqual expectedLabels
	}

	"Aliases" should "be extracted" in {
		val testAliases = Json.parse(TestData.rawWikidataEntries()).as[List[JsValue]]
			.map(WikiDataImport.extractAliases)
		val expectedAliases = TestData.entityAliases()
		testAliases shouldEqual expectedAliases
	}

	"Entity values" should "be filled in" in {
		val testEntities = Json.parse(TestData.rawWikidataEntries()).as[List[JsValue]]
		    .map(WikiDataImport.fillEntityValues)
		val expectedEntities = TestData.filledWikidataEntities()
		testEntities shouldEqual expectedEntities
	}

	"Claim values" should "be extracted" in {
		val claimJson = Json.parse(TestData.claimData()).as[JsObject]
		claimJson.value.foreach { case (dataType, jsonData) =>
			val extractedData = WikiDataImport.extractClaimValues(jsonData)
			val expectedValue = TestData.dataTypeValues(dataType)
			extractedData shouldEqual expectedValue
		}
	}

	"Wikidata entities" should "be parsed" in {
		val testEntities = Json.parse(TestData.rawWikidataEntries()).as[List[JsValue]]
		    .map(_.toString)
		    .map(WikiDataImport.parseEntity)
		val expectedEntities = TestData.parsedWikidataEntities()
		testEntities shouldEqual expectedEntities
	}

	"Property ids" should "be translated" in {
		val propertyMap = TestData.propertyMap()
		val testEntities = TestData.parsedWikidataEntities()
		    .map(WikiDataImport.translatePropertyIDs(_, propertyMap))
		val expectedEntities = TestData.translatedWikidataEntities()
		testEntities shouldEqual expectedEntities
	}

	"Property map" should "be built" in {
		val properties = sc.parallelize(TestData.propertyEntities())
		val propertyMap = WikiDataImport.buildPropertyMap(properties)
		val expectedMap = TestData.propertyMap()
		propertyMap shouldEqual expectedMap
	}
}
