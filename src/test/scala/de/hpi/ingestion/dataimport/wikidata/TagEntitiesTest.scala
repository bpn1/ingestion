package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class TagEntitiesTest extends FlatSpec with SharedSparkContext with Matchers {
	"Shorter paths" should "be added" in {
		val oldClasses = TestData.oldClassMap()
		val newClasses = TestData.newClassMap()
		val classMap = TagEntities.addShorterPaths(newClasses, oldClasses)
		val expectedMap = TestData.classMap()
		classMap shouldEqual expectedMap
	}

	"Subclass map" should "be built" in {
		val entries = sc.parallelize(TestData.subclassOfProperties())
		val classMap = TagEntities.buildSubclassMap(entries, TestData.classesToTag())
		val expectedMap = TestData.classMap()
		classMap shouldEqual expectedMap
	}

	"Wikidata entity" should "be properly translated into SubclassEntry" in {
		val entries = TestData.classWikidataEntities()
		    .map(TagEntities.translateToSubclassEntry)
		val expectedEntries = TestData.subclassEntries()
		entries shouldEqual expectedEntries
	}

	"Instance-of entities" should "be updated" in {
		val entries = sc.parallelize(TestData.subclassEntries())
		val updatedEntries = TagEntities.updateEntities(entries, TestData.classMap()).collect.toSet
		val expectedEntries = TestData.updatedInstanceOfProperties().toSet
		updatedEntries shouldEqual expectedEntries
	}

	it should "be updated correctly" in {
		val entries = TestData.validInstanceOfProperties()
			.map(TagEntities.updateInstanceOfProperty(_, TestData.classMap()))
		val expectedEntries = TestData.updatedInstanceOfProperties()
		entries shouldEqual expectedEntries
	}
}
