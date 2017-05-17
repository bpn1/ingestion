package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikiDataDataLakeImportTest extends FlatSpec with SharedSparkContext with Matchers {
	val entity = TestData.testEntity()

	"translateToSubject" should "map label, aliases and category correctly" in {
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies)
		subject.name shouldEqual entity.label
		subject.aliases shouldEqual entity.aliases
		subject.category shouldEqual entity.instancetype
	}

	it should "normalize the data attributes" in {
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
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val strategies = TestData.strategies
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version, mapping, strategies)
		subject.properties("testProperty") shouldEqual List("test")
	}
}
