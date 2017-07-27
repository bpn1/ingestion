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

package de.hpi.ingestion.dataimport.wikipedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class WikipediaImportTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {

	"XML" should "be parsed" in {
		val entries = TestData.pageXML.map(WikipediaImport.parseXML)
		val expected = TestData.wikipediaEntries
		entries shouldEqual expected
	}

	"Wikipedia" should "be parsed" in {
		val xmlInput = List(sc.parallelize(TestData.pageXML)).toAnyRDD()
		val entries = WikipediaImport.run(xmlInput, sc).fromAnyRDD[WikipediaEntry]().head
		val expected = sc.parallelize(TestData.wikipediaEntries)
		assertRDDEquals(entries, expected)
	}
}
