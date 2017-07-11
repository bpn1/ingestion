package de.hpi.ingestion.dataimport.kompass

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._

class KompassDataLakeImportTest extends FlatSpec with Matchers with SharedSparkContext {
	"extractAddress" should "extract and normalize street, postal, city and country from the address" in {
		val addresses = TestData.unnormalizedAddresses
		val normalizedAddresses = addresses.map(KompassDataLakeImport.extractAddress)
		val expected = TestData.normalizedAddresses
		normalizedAddresses shouldEqual expected
	}

	/*"normalizeAttribute" should "normalize the values of a given attribute" in {
		val attributes = TestData.unnormalizedAttributes
		val strategies = TestData.strategies
		val normalizedAttributes = attributes.map { case (attribute, values) =>
			attribute -> KompassDataLakeImport.normalizeAttribute(attribute, values, strategies)
		}
		val expected = TestData.normalizedAttributes
		normalizedAttributes shouldEqual expected
	}*/
	/*

	"translateToSubject" should "map the entity label on subject name" in {
		val entities = TestData.dbpediaEntities
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = KompassDataLakeImport.classifier
		val translatedSubjects = entities.map { entity =>
			DBpediaDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
		}
		val expectedSubjects = TestData.translatedSubjects
		translatedSubjects should not be empty
		(translatedSubjects, expectedSubjects).zipped.foreach { case (subject, expected) =>
			subject.name shouldEqual expected.name
			subject.category shouldEqual expected.category
		}
	}*/

	/*it should "normalize the data attributes" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = KompassDataLakeImport.classifier
		val subject = KompassDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("id_wikidata") shouldEqual List("Q123")
		subject.properties("id_wikipedia") shouldEqual List("List von Autoren")
		subject.properties("id_dbpedia") shouldEqual List("List von Autoren")
		subject.properties("id_viaf") shouldEqual List("X123", "Y123")
		subject.properties("geo_country") shouldEqual List("CH")
		subject.properties("gen_employees") shouldEqual List("12", "13")
		subject.properties shouldNot contain key "id_lccn"
	}

	it should "copy all old data attributes" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = KompassDataLakeImport.classifier
		val subject = KompassDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("testProperty") shouldEqual List("test")
	}

	it should "properly merge coordinates" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = KompassDataLakeImport.classifier
		val subject = KompassDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("geo_coords") shouldEqual List("52;100")
	}

	it should "extract the legal form" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val classifier = KompassDataLakeImport.classifier
		val subject = KompassDataLakeImport.translateToSubject(entity, version, mapping, strategies, classifier)
		subject.properties("gen_legal_form") shouldEqual List("GmbH")
	}
*/
}
