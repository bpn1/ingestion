package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikiDataDataLakeImportTest extends FlatSpec with SharedSparkContext with Matchers {
	"filterEntities" should "filter entities without an instance type" in {
		val entities = TestData.unfilteredEntities
		val filteredEntities = entities.filter(WikiDataDataLakeImport.filterEntities)
		val expected = TestData.filteredEntities
		filteredEntities shouldEqual expected
	}

	"normalizeAttribute" should "the values of a given attribute" in {
		val attributes = Map(
			"gen_sectors" -> TestData.unnormalizedSectors,
			"geo_coords" -> TestData.unnormalizedCoordinates,
			"geo_country" -> TestData.unnormalizedLocations,
			"geo_city" -> TestData.unnormalizedLocations,
			"gen_employees" -> TestData.unnormalizedEmployees
		)
		val strategies = TestData.strategies
		val normalizedAttributes = attributes.map { case (attribute, values) =>
			attribute -> WikiDataDataLakeImport.normalizeAttribute(attribute, values, strategies)
		}
		val expected = Map(
			"gen_sectors" -> TestData.mappedSectors,
			"geo_coords" -> TestData.normalizedCoordinates,
			"geo_country" -> TestData.normalizedLocations,
			"geo_city" -> TestData.normalizedLocations,
			"gen_employees" -> TestData.normalizedEmployees
		)
		normalizedAttributes shouldEqual expected
	}

	"translateToSubject" should "map label, aliases and category correctly" in {
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val translatedSubjects = TestData.wikidataEntities.map { entity =>
			(entity, WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies))
		}
		translatedSubjects should not be empty
		translatedSubjects.foreach { case (entity, subject) =>
			subject.name shouldEqual entity.label
			subject.aliases shouldEqual entity.aliases
			subject.category shouldEqual entity.instancetype
		}
	}

	it should "normalize the data attributes" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies)
		subject.properties("id_wikidata") shouldEqual List("Q21110253")
		subject.properties("id_wikipedia") shouldEqual List("testwikiname")
		subject.properties("id_dbpedia") shouldEqual List("testwikiname")
		subject.properties("id_viaf") shouldEqual List("X123")
		subject.properties("gen_sectors") shouldEqual TestData.mappedSectors
		subject.properties("geo_coords") shouldEqual TestData.normalizedCoordinates
		subject.properties("geo_country") shouldEqual TestData.normalizedLocations
		subject.properties("geo_city") shouldEqual TestData.normalizedLocations
		subject.properties("gen_employees") shouldEqual TestData.normalizedEmployees
		subject.properties shouldNot contain key "id_lccn"
	}

	it should "copy all old data attributes" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies)
		subject.properties("testProperty") shouldEqual List("test")
	}
}
