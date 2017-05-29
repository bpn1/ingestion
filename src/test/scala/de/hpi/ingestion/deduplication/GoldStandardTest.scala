package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class GoldStandardTest extends FlatSpec
	with Matchers
	with SharedSparkContext
	with RDDComparisons
{
	"keyBySingleProperty" should "key a Subject RDD by value of a given property" in {
		val rdd = sc.parallelize(TestData.dbpediaList)
		val propertyKeyRDD = GoldStandard.keyBySingleProperty(rdd, "id_dbpedia")
		val expected = TestData.propertyKeyDBpedia(sc)
		assertRDDEquals(expected, propertyKeyRDD)
	}

	"joinBySingleProperty" should "join DBpedia and WikiData on a given property" in {
		val dbpedia = sc.parallelize(TestData.dbpediaList)
		val wikidata = sc.parallelize(TestData.wikidataList)
		val joinedRDD = GoldStandard.joinBySingleProperty("id_wikidata", dbpedia, wikidata)
		val expected = TestData.joinedWikiData(sc)
		assertRDDEquals(expected, joinedRDD)
	}

	"join" should "join DBpedia and WikiData on different ids" in {
		val dbpedia = sc.parallelize(TestData.dbpediaList)
		val wikidata = sc.parallelize(TestData.wikidataList)
		val joinedRDD = GoldStandard.join(dbpedia, wikidata)
		val expected = TestData.joinedDBpediaWikiData(sc)
		assertRDDEquals(expected, joinedRDD)
	}
}
