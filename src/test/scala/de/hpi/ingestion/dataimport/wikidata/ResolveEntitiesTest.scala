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

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class ResolveEntitiesTest
	extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Wikidata entities" should "be flattened properly" in {
		val job = new ResolveEntities
		val entities = TestData.unresolvedWikidataEntities()
			.flatMap(job.flattenWikidataEntity)
		val expectedEntities = TestData.flattenedWikidataEntries()
		entities shouldEqual expectedEntities
	}

	"Wikidata id regex" should "find wikidata id values" in {
		val job = new ResolveEntities
		val entries = TestData.flattenedWikidataEntries()
			.filter(job.containsWikidataIdValue)
		val expectedEntries = TestData.wikidataIdEntries()
		entries shouldEqual expectedEntries
	}

	"Wikidata unit if regex" should "find wikidata ids as measurement units" in {
		val job = new ResolveEntities
		val entries = TestData.flattenedWikidataEntries()
			.filter(job.hasUnitValue)
		val expectedEntries = TestData.unitWikidataIdEntries()
		entries shouldEqual expectedEntries
	}

	"Wikidata unit values" should "be split into a joinable format" in {
		val job = new ResolveEntities
		val entries = TestData.unitWikidataIdEntries()
			.map(job.splitUnitValue)
		val expectedEntries = TestData.splitUnitWikidataIdEntries()
		entries shouldEqual expectedEntries
	}

	"Wikidata entities to resolve" should "be found" in {
		val job = new ResolveEntities
		val entries = TestData.unfilteredWikidataEntities()
			.filter(job.shouldBeResolved)
		val expectedEntries = TestData.filteredWikidataEntities()
		entries shouldEqual expectedEntries
	}

	"Wikidata name data" should "be extracted" in {
		val job = new ResolveEntities
		val entities = TestData.unfilteredWikidataEntities()
		    .map(job.extractNameData)
		val expectedEntries = TestData.entityNameData()
		entities shouldEqual expectedEntries
	}

	"Wikidata id rdd" should "be joined with name rdd" in {
		val job = new ResolveEntities
		val entities = sc.parallelize(TestData.wikidataIdEntries()
		    .map(job.makeJoinable))
		val names = sc.parallelize(TestData.entityNameData())
		val joinedData = job.joinIdRDD(entities, names)
		val expected = sc.parallelize(TestData.resolvedWikidataIdEntries())
		assertRDDEquals(joinedData, expected)
	}

	"Wikidata unit rdd" should "be joined with name rdd" in {
		val job = new ResolveEntities
		val entities = sc.parallelize(TestData.splitUnitWikidataIdEntries())
		val names = sc.parallelize(TestData.entityNameData())
		val joinedData = job.joinUnitRDD(entities, names)
		val expected = sc.parallelize(TestData.resolvedUnitWikidataIdEntries())
		assertRDDEquals(joinedData, expected)
	}

	"Property map" should "be rebuild" in {
		val job = new ResolveEntities
		val entries = TestData.flattenedWikidataEntries()
		val rebuiltData = job.rebuildProperties(sc.parallelize(entries))
			.collect
			.head._2
		val expected = TestData.unresolvedWikidataEntities()
			.map(_.data)
		    .head
		rebuiltData shouldEqual expected
	}
}
