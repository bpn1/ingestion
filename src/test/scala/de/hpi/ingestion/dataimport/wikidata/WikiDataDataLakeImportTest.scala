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
		val attributes = TestData.unnormalizedAttributes
		val strategies = TestData.strategies
		val normalizedAttributes = attributes.map { case (attribute, values) =>
			attribute -> WikiDataDataLakeImport.normalizeAttribute(attribute, values, strategies)
		}
		val expected = TestData.normalizedAttributes
		normalizedAttributes shouldEqual expected
	}

	"translateToSubject" should "map label, aliases and category correctly" in {
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = WikiDataDataLakeImport.classifier
		val translatedSubjects = TestData.wikidataEntities.map { entity =>
			entity -> WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
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
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = WikiDataDataLakeImport.classifier
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
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
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = WikiDataDataLakeImport.classifier
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("testProperty") shouldEqual List("test")
	}

	it should "extract the legal form" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = WikiDataDataLakeImport.classifier
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("gen_legal_form") shouldEqual TestData.normalizedLegalForm ::: List("AG")
	}
}
