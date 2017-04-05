package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class TagEntitiesTest extends FlatSpec with SharedSparkContext with Matchers {
	"Shorter paths" should "be added" in {
		val oldClasses = TestData.oldClassMap()
		val newClasses = TestData.newClassMap()
		val classMap = TagEntities.addShorterPaths(newClasses, oldClasses)
		val expectedMap = TestData.classMap()
		classMap shouldBe expectedMap
	}

	"Subclass map" should "be built" in {
		val entries = sc.parallelize(TestData.subclassOfProperties())
		val classMap = TagEntities.buildSubclassMap(entries, TestData.classesToTag())
		val expectedMap = TestData.classMap()
		classMap shouldBe expectedMap
	}

	"Wikidata entity" should "be properly translated into SubclassEntry" in {
		val entries = TestData.classWikidataEntities()
		    .map(TagEntities.translateToSubclassEntry)
		val expectedEntries = TestData.subclassEntries()
		entries shouldBe expectedEntries
	}

	"Instance-of property" should "be checked correctly" in {
		val entries = TestData.subclassEntries()
			.filter(TagEntities.isInstanceOf(_, TestData.classMap()))
		val expectedEntries = TestData.validInstanceOfProperties()
		entries shouldBe expectedEntries
	}

	it should "be updated correctly" in {
		val entries = TestData.validInstanceOfProperties()
			.map(TagEntities.updateInstanceOfProperty(_, TestData.classMap()))
		val expectedEntries = TestData.updatedInstanceOfProperties()
		entries shouldBe expectedEntries
	}
}
