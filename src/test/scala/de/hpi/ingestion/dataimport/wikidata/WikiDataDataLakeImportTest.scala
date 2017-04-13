package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class WikiDataDataLakeImportTest extends FlatSpec with SharedSparkContext with Matchers {
	val entity = TestData.testEntity

	"translateToSubject" should "map label, aliases and category correctly" in {
		val version = TestData.version(sc)
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version)
		subject.name shouldEqual entity.label
		subject.aliases shouldEqual entity.aliases
		subject.category shouldEqual entity.instancetype
	}

	it should "normalize the data attributes" in {
		val version = TestData.version(sc)
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version)
		subject.properties should contain ("id_wikidata" -> List("Q21110253"))
		subject.properties should contain ("id_wikipedia" -> List("testwikiname"))
		subject.properties should contain ("id_dbpedia" -> List("testwikiname"))
		subject.properties should contain ("id_viaf" -> List("X123"))
		subject.properties shouldNot contain key "id_lccn"
	}

	it should "copy all old data attributes" in {
		val version = TestData.version(sc)
		val subject = WikiDataDataLakeImport.translateToSubject(entity, version)
		subject.properties should contain ("testProperty" -> List("test"))
	}
}
