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
import org.scalatest.{FlatSpec, Matchers}

class WikidataDataLakeImportTest extends FlatSpec with SharedSparkContext with Matchers {
	"filterEntities" should "filter entities without an instance type" in {
		val job = new WikidataDataLakeImport
		val entities = TestData.unfilteredEntities
		val filteredEntities = entities.filter(job.filterEntities)
		val expected = TestData.filteredEntities
		filteredEntities shouldEqual expected
	}

	"normalizeAttribute" should "the values of a given attribute" in {
		val job = new WikidataDataLakeImport
		val attributes = TestData.unnormalizedAttributes
		val strategies = TestData.strategies
		val normalizedAttributes = attributes.map { case (attribute, values) =>
			attribute -> job.normalizeAttribute(attribute, values, strategies)
		}
		val expected = TestData.normalizedAttributes
		normalizedAttributes shouldEqual expected
	}

	"translateToSubject" should "map label, aliases and category correctly" in {
		val job = new WikidataDataLakeImport
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = job.classifier
		val translatedSubjects = TestData.wikidataEntities.map { entity =>
			entity -> job.translateToSubject(entity, version, mapping, strategies, classifier)
		}
		translatedSubjects should not be empty
		translatedSubjects.foreach { case (entity, subject) =>
			subject.name shouldEqual entity.label
			subject.aliases shouldEqual entity.aliases
			entity.instancetype match {
				case Some(x) => subject.category should contain ("business")
				case None => subject.category shouldBe empty
			}
		}
	}

	it should "normalize the data attributes" in {
		val job = new WikidataDataLakeImport
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = job.classifier
		val subject = job.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("id_wikidata") shouldEqual List("Q21110253")
		subject.properties("id_wikipedia") shouldEqual List("testwikiname")
		subject.properties("id_dbpedia") shouldEqual List("testwikiname")
		subject.properties("id_viaf") shouldEqual List("X123")
		subject.properties("gen_sectors") shouldEqual TestData.mappedSectors
		subject.properties("geo_coords") shouldEqual TestData.normalizedCoords
		subject.properties("geo_country") shouldEqual TestData.normalizedCountries
		subject.properties("geo_city") shouldEqual TestData.normalizedCities
		subject.properties("gen_employees") shouldEqual TestData.normalizedEmployees
		subject.properties("gen_urls") shouldEqual TestData.normalizedURLs
		subject.properties shouldNot contain key "id_lccn"
	}

	it should "copy all old data attributes" in {
		val job = new WikidataDataLakeImport
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = job.classifier
		val subject = job.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("testProperty") shouldEqual List("test")
	}

	it should "extract the legal form" in {
		val job = new WikidataDataLakeImport
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = job.classifier
		val subject = job.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("gen_legal_form") shouldEqual List("AG")
	}
}
