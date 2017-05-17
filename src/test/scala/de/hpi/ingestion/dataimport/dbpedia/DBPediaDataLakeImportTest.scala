package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class DBpediaDataLakeImportTest extends FlatSpec with Matchers with SharedSparkContext {
	"translateToSubject" should "map the entity label on subject name" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val subject = DBpediaDataLakeImport.translateToSubject(entity, version, mapping)
		subject.name shouldEqual entity.label
	}

	it should "normalize the data attributes" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val subject = DBpediaDataLakeImport.translateToSubject(entity, version, mapping)
		subject.properties("id_wikidata") shouldEqual List("Q123")
		subject.properties("id_wikipedia") shouldEqual List("dbpedia-de:List_von_Autoren")
		subject.properties("id_dbpedia") shouldEqual List("dbpedia-de:List_von_Autoren")
		subject.properties("id_viaf") shouldEqual List("X123", "Y123")
		subject.properties("geo_country") shouldEqual List("Koblenz")
		subject.properties("gen_employees") shouldEqual List("12", "13")
		subject.properties shouldNot contain key "id_lccn"
	}

	it should "copy all old data attributes" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val subject = DBpediaDataLakeImport.translateToSubject(entity, version, mapping)
		subject.properties("testProperty") shouldEqual List("test")
	}

	it should "properly merge coordinates" in {
		val entity = TestData.testEntity
		val version = TestData.version(sc)
		val mapping = TestData.mapping
		val subject = DBpediaDataLakeImport.translateToSubject(entity, version, mapping)
		subject.properties("geo_coords") shouldEqual List("52", "100")
	}
}
