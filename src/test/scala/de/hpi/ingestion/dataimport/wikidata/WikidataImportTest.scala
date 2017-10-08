/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest._
import play.api.libs.json.{JsObject, JsValue, Json}

class WikidataImportTest extends FlatSpec with SharedSparkContext with Matchers {
	"Json" should "be cleaned and valid json" in {
		val job = new WikidataImport
		// invalid json would cause an exception
		TestData.rawWikidataEntries()
			.map(job.cleanJSON)
			.filter(_.nonEmpty)
			.map(Json.parse)
	}

	"Data types" should "be parsed" in {
		val job = new WikidataImport
		val claimJson = Json.parse(TestData.claimData()).as[JsObject]
		claimJson.value.foreach { case (dataType, jsonData) =>
			val extractedData = job.parseDataType(Option(dataType), jsonData)
			val expectedValue = TestData.dataTypeValues(dataType)
			extractedData shouldEqual expectedValue
		}
	}

	"Labels" should "be extracted" in {
		val job = new WikidataImport
		val testEntities = Json.parse(TestData.rawTestEntries()).as[List[JsValue]]
			.map(value => job.extractLabels(value, job.extractString(value, List("type"))))
		val expectedLabels = TestData.entityLabels()
		testEntities shouldEqual expectedLabels
	}

	"Aliases" should "be extracted" in {
		val job = new WikidataImport
		val testAliases = Json.parse(TestData.rawTestEntries()).as[List[JsValue]]
			.map(job.extractAliases)
		val expectedAliases = TestData.entityAliases()
		testAliases shouldEqual expectedAliases
	}

	"Entity values" should "be filled in" in {
		val job = new WikidataImport
		val testEntities = Json.parse(TestData.rawTestEntries()).as[List[JsValue]]
			.map(job.fillSimpleValues)
		val expectedEntities = TestData.filledWikidataEntities()
		testEntities shouldEqual expectedEntities
	}

	"Claim values" should "be extracted" in {
		val job = new WikidataImport
		val claimJson = Json.parse(TestData.claimData()).as[JsObject]
		claimJson.value.foreach { case (dataType, jsonData) =>
			val extractedData = job.extractClaimValues(jsonData)
			val expectedValue = TestData.dataTypeValues(dataType)
			extractedData shouldEqual expectedValue
		}
	}

	"Wikidata entities" should "be parsed" in {
		val job = new WikidataImport
		val testEntities = Json.parse(TestData.rawTestEntries()).as[List[JsValue]]
			.map(job.fillEntityValues)
		val expectedEntities = TestData.parsedWikidataEntities()
		testEntities shouldEqual expectedEntities
	}

	"Property ids" should "be translated" in {
		val propertyMap = TestData.propertyMap()
		val testEntities = TestData.parsedWikidataEntities()
			.map(WikidataImport.translatePropertyIDs(_, propertyMap))
		val expectedEntities = TestData.translatedWikidataEntities()
		testEntities shouldEqual expectedEntities
	}

	"Property map" should "be built" in {
		val properties = sc.parallelize(TestData.propertyEntities())
		val propertyMap = WikidataImport.buildPropertyMap(properties)
		val expectedMap = TestData.propertyMap()
		propertyMap shouldEqual expectedMap
	}
}
