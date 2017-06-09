package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class ResolveEntitiesTest
	extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Wikidata entities" should "be flattened properly" in {
		val entities = TestData.unresolvedWikidataEntities()
			.flatMap(ResolveEntities.flattenWikidataEntity)
		val expectedEntities = TestData.flattenedWikidataEntries()
		entities shouldEqual expectedEntities
	}

	"Wikidata id regex" should "find wikidata id values" in {
		val entries = TestData.flattenedWikidataEntries()
			.filter(ResolveEntities.containsWikidataIdValue)
		val expectedEntries = TestData.wikidataIdEntries()
		entries shouldEqual expectedEntries
	}

	"Wikidata unit if regex" should "find wikidata ids as measurement units" in {
		val entries = TestData.flattenedWikidataEntries()
			.filter(ResolveEntities.hasUnitValue)
		val expectedEntries = TestData.unitWikidataIdEntries()
		entries shouldEqual expectedEntries
	}

	"Wikidata unit values" should "be split into a joinable format" in {
		val entries = TestData.unitWikidataIdEntries()
			.map(ResolveEntities.splitUnitValue)
		val expectedEntries = TestData.splitUnitWikidataIdEntries()
		entries shouldEqual expectedEntries
	}

	"Wikidata entities to resolve" should "be found" in {
		val entries = TestData.unfilteredWikidataEntities()
			.filter(ResolveEntities.shouldBeResolved)
		val expectedEntries = TestData.filteredWikidataEntities()
		entries shouldEqual expectedEntries
	}

	"Wikidata name data" should "be extracted" in {
		val entities = TestData.unfilteredWikidataEntities()
		    .map(ResolveEntities.extractNameData)
		val expectedEntries = TestData.entityNameData()
		entities shouldEqual expectedEntries
	}

	"Wikidata id rdd" should "be joined with name rdd" in {
		val entities = sc.parallelize(TestData.wikidataIdEntries()
		    .map(ResolveEntities.makeJoinable))
		val names = sc.parallelize(TestData.entityNameData())
		val joinedData = ResolveEntities.joinIdRDD(entities, names)
		val expected = sc.parallelize(TestData.resolvedWikidataIdEntries())
		assertRDDEquals(joinedData, expected)
	}

	"Wikidata unit rdd" should "be joined with name rdd" in {
		val entities = sc.parallelize(TestData.splitUnitWikidataIdEntries())
		val names = sc.parallelize(TestData.entityNameData())
		val joinedData = ResolveEntities.joinUnitRDD(entities, names)
		val expected = sc.parallelize(TestData.resolvedUnitWikidataIdEntries())
		assertRDDEquals(joinedData, expected)
	}

	"Property map" should "be rebuild" in {
		val entries = TestData.flattenedWikidataEntries()
		val rebuiltData = ResolveEntities.rebuildProperties(sc.parallelize(entries))
			.collect
			.head._2
		val expected = TestData.unresolvedWikidataEntities()
			.map(_.data)
		    .head
		rebuiltData shouldEqual expected
	}
}
