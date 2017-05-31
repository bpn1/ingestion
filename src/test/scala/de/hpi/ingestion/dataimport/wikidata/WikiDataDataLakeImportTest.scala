package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikiDataDataLakeImportTest extends FlatSpec with SharedSparkContext with Matchers {
	"translateToSubject" should "map label, aliases and category correctly" in {
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val translatedSubjects = TestData.wikidataEntities().map(entity =>
			(entity, WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies)))
		translatedSubjects should not be empty
		translatedSubjects.foreach { case (entity, subject) =>
			subject.name shouldEqual entity.label
			subject.aliases shouldEqual entity.aliases
			subject.category shouldEqual entity.instancetype
		}
	}

	it should "normalize the data attributes" in {
		val entity = TestData.testEntity()
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies)
		subject.properties("id_wikidata") shouldEqual List("Q21110253")
		subject.properties("id_wikipedia") shouldEqual List("testwikiname")
		subject.properties("id_dbpedia") shouldEqual List("testwikiname")
		subject.properties("id_viaf") shouldEqual List("X123")
		subject.properties shouldNot contain key "id_lccn"
	}

	it should "copy all old data attributes" in {
		val entity = TestData.testEntity()
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies)
		subject.properties("testProperty") shouldEqual List("test")
	}
}
