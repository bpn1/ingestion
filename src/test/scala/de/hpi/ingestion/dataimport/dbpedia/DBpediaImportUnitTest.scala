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

package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import org.scalatest.{FlatSpec, Matchers}

class DBpediaImportUnitTest extends FlatSpec with SharedSparkContext with Matchers {
	"tokenize" should "return a three element long list" in {
		val tokens = DBpediaImport.tokenize(TestData.line)
		tokens should have length 3
	}

	it should "return a three element long list with a shorter input" in {
		TestData.shorterLineList.foreach{ line =>
			val tokens = DBpediaImport.tokenize(line)
			tokens should have length 3
		}
	}

	it should "return a three element long list with a longer input" in {
		TestData.longerLineList.foreach{ line =>
			val tokens = DBpediaImport.tokenize(line)
			tokens should have length 3
		}
	}

	it should "tokenize the triple correctly" in {
		val parsedTokens = DBpediaImport.tokenize(TestData.line)
		val expected = TestData.lineTokens
		parsedTokens shouldEqual expected
	}

	"cleanURL" should "replace all prefixes" in {
		val line = sc.parallelize(List(TestData.line))
		val prefixList = TestData.prefixesList
		val cleanList = DBpediaImport.dbpediaToCleanedTriples(line, prefixList).collect.toList.head
		cleanList.head should startWith ("dbpedia-de:")
		cleanList(1) should startWith ("dct:")
		cleanList(2) should startWith ("dbpedia-de:")
	}

	"extractWikidataId" should "extract the wikidata id from a list" in {
		val owlSameAs = List("yago:X", "wikidata:123", "wikpedia:5")
		val id = DBpediaImport.extractWikidataId(owlSameAs)
		val expected = Option("123")
		id shouldEqual expected
	}

	it should "return None if list is empty" in {
		val id = DBpediaImport.extractWikidataId(Nil)
		id shouldEqual None
	}

	it should "return None if it could not be found" in {
		val owlSameAs = List("yago:X", "wikipedia:5")
		val id = DBpediaImport.extractWikidataId(owlSameAs)
		id shouldEqual None
	}

	"extractInstancetype" should "extract the right type from a list" in {
		val rdfTypes = List("owl:Thing", "dbo:Agent", "dbo:Organisation", "dbo:Company")
		val organisations = TestData.organisations
		val instanceType = DBpediaImport.extractInstancetype(rdfTypes, organisations)
		val expected = Option("Company")
		instanceType shouldEqual expected
	}

	it should "return None if given list is empty" in {
		val organisations = TestData.organisations
		val instanceType = DBpediaImport.extractInstancetype(Nil, organisations)
		instanceType shouldEqual None
	}

	it should "return None if no sublass of organisation could be found" in {
		val rdfTypes = List("owl:Thing", "dbo:Agent", "dbo:Family")
		val organisations = TestData.organisations
		val instanceType = DBpediaImport.extractInstancetype(rdfTypes, organisations)
		instanceType shouldEqual None
	}

	"extractProperties" should "create a DBpediaEntity from properties" in {
		val name = "Test Entity"
		val entity = DBpediaImport.extractProperties(name, TestData.properties, TestData.organisations)
		val expected = TestData.parsedEntity(name)
		entity shouldEqual expected
	}

	"DBpedia entities" should "be extracted" in {
		val job = new DBpediaImport
		job.dbpediaDump = sc.parallelize(TestData.rawTriples())
		job.run(sc)
		val entities = job.dbpediaEntities.collect.toSet
		val expectedEntitites = TestData.parsedDBpediaEntities()
		entities shouldEqual expectedEntitites
	}
}
