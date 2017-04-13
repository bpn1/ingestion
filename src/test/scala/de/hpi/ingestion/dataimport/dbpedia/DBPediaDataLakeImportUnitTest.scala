package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class DBPediaDataLakeImportUnitTest extends FlatSpec with Matchers with SharedSparkContext {
	val version = TestData.version(sc)
	val entity = TestData.testEntity
	val subject = DataLakeImportDBpedia.translateToSubject(entity, version)

	"translateToSubject" should "map the entity label on subject name" in {
		subject.name shouldEqual entity.label
	}

	it should "normalize the data attributes" in {
		subject.properties should contain ("id_wikidata" -> List("Q123"))
		subject.properties should contain ("id_wikipedia" -> List("dbpedia-de:List_von_Autoren"))
		subject.properties should contain ("id_dbpedia" -> List("dbpedia-de:List_von_Autoren"))
		subject.properties should contain ("id_viaf" -> List("X123", "Y123"))
		subject.properties shouldNot contain key "id_lccn"
	}

	it should "properly merge coordinates" in {
		subject.properties should contain ("geo_coords" -> List("52", "100"))
	}
}
