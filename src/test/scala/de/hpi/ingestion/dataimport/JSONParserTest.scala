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

package de.hpi.ingestion.dataimport

import java.util.GregorianCalendar
import de.hpi.ingestion.dataimport.mock.MockJSONParser
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json.{JsBoolean, JsNumber, JsString}

class JSONParserTest extends FlatSpec with Matchers {
	"Values" should "be extracted" in {
		val jsonObject = TestData.parsedJSON()
		val parser = new MockJSONParser
		parser.getValue(jsonObject, List("key 1")) should contain (JsString("string 1"))
		parser.getValue(jsonObject, List("key 1", "key 2")) shouldBe empty
		parser.getValue(jsonObject, List("key 2")) should contain (JsNumber(123.0))
		parser.getValue(jsonObject, List("key 3")) should contain (JsBoolean(false))
		parser.getValue(jsonObject, List("key 4")) shouldBe defined
	}

	"JSON" should "be cleaned" in {
		val parser = new MockJSONParser
		val cleanedJson = TestData.dirtyJsonLines()
		    .map(parser.cleanJSON)
		val expectedJson = TestData.cleanenedJsonLines()
		cleanedJson shouldEqual expectedJson
	}

	"Data types" should "be extracted" in {
		val parser = new MockJSONParser
		val jsonObject = TestData.parsedJSON()
		val expectedList = List(JsString("value 1"), JsString("value 2"), JsString("value 3"))
		val exepectedMap = Map("nestedkey 1" -> JsString("subvalue 1"), "nestedkey 2" -> JsString("subvalue 2"))
		val expectedDate = new GregorianCalendar(2017, 11, 24, 0, 0, 0).getTime
		val expectedNestedMap = Map(
			"nestedkey 1.nested2key 1" -> "subvalue 1",
			"nestedkey 1.nested2key 2" -> "subvalue 2",
			"nestedkey 2.nested2key 1" -> "subvalue 1"
		)
		parser.extractString(jsonObject, List("key 1")) should contain ("string 1")
		parser.extractDouble(jsonObject, List("key 2")) should contain ("123.0")
		parser.extractList(jsonObject, List("key 4")) shouldEqual expectedList
		parser.extractStringList(jsonObject, List("key 4")) shouldEqual List("value 1", "value 2", "value 3")
		parser.extractMap(jsonObject, List("key 6")) shouldEqual exepectedMap
		parser.extractBoolean(jsonObject, List("key 3")) should contain (false)
		parser.extractDate(jsonObject, List("key 5"), "yyyy-MM-dd") should contain (expectedDate)
		parser.extractMap(jsonObject, List("key 7")).flatMap(parser.flattenNestedMap) shouldEqual expectedNestedMap
		assertThrows[IllegalArgumentException] { parser.flattenNestedMap("test key", JsNumber(1.0)) }
	}
}
