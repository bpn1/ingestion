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

package de.hpi.ingestion.deduplication

import java.util.UUID
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class GoldStandardTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {
	"keyBySingleProperty" should "key a Subject RDD by value of a given property" in {
		val rdd = sc.parallelize(TestData.dbpediaList)
		val propertyKeyRDD = GoldStandard.keyBySingleProperty(rdd, "id_dbpedia")
		val expected = TestData.propertyKeyDBpedia(sc)
		assertRDDEquals(expected, propertyKeyRDD)
	}

	"joinBySingleProperty" should "join DBpedia and Wikidata on a given property" in {
		val dbpedia = sc.parallelize(TestData.dbpediaList)
		val wikidata = sc.parallelize(TestData.wikidataList)
		val joinedRDD = GoldStandard.joinBySingleProperty("id_wikidata", dbpedia, wikidata)
		val expected = TestData.joinedWikidata(sc)
		assertRDDEquals(expected, joinedRDD)
	}

	"join" should "join DBpedia and Wikidata on different ids" in {
		val dbpedia = sc.parallelize(TestData.dbpediaList)
		val wikidata = sc.parallelize(TestData.wikidataList)
		val joinedRDD = GoldStandard.join(dbpedia, wikidata)
		val expected = TestData.joinedDBpediaWikidata(sc)
		assertRDDEquals(expected, joinedRDD)
	}

	"Wikidata and DBpedia" should "be joined" in {
		val dbpedia = sc.parallelize(TestData.dbpediaList)
		val wikidata = sc.parallelize(TestData.wikidataList)
		val input = List(dbpedia).toAnyRDD() ++ List(wikidata).toAnyRDD()
		val result = GoldStandard.run(input, sc).fromAnyRDD[(UUID, UUID)]().head
		val expectedResult = TestData.joinedDBpediaWikidata(sc)
		assertRDDEquals(expectedResult, result)
	}
}
