package de.hpi.ingestion.dataimport.dbpedia

import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity

class DBpediaImportRDDTest extends FlatSpec with SharedSparkContext with RDDComparisons {

	"Triples" should "be tokenized into three elements" in {
		TestData.turtleRDD(sc)
		    .map(DBPediaImport.tokenize)
		    .collect
		    .foreach(tripleList => assert(tripleList.size == 3))
	}

	they should "have namespace prefixes after cleaning" in {
		val parsed = TestData.turtleRDD(sc)
			.map(DBPediaImport.tokenize)
			.collect
		    .map(tripleList =>
				tripleList.map(el => DBPediaImport.cleanURL(el, TestData.prefixesList())))
			.map { case List(a, b, c) => (a, b, c) }
		    .toList
		val triples = TestData.tripleRDD(sc)
			.map(el => (el._1, el._2._1, el._2._2))
			.collect
			.toList
		assert(parsed == triples)
	}

	"DBpediaEntities" should "not be empty" in {
		val rdd = TestData.tripleRDD(sc)
			.groupByKey
			.map(tuple => DBPediaImport.extractProperties(tuple._1, tuple._2.toList))
		assert(rdd.count > 0)
	}

	they should "be instances of DBpediaEntitiy" in {
		TestData.tripleRDD(sc)
			.groupByKey
			.map(tuple => DBPediaImport.extractProperties(tuple._1, tuple._2.toList))
		    .collect
		    .foreach(entity => assert(entity.isInstanceOf[DBPediaEntity]))
	}

	they should "contain the same information as the triples" in {
		val entities = TestData.tripleRDD(sc)
			.groupByKey
			.map(tuple => DBPediaImport.extractProperties(tuple._1, tuple._2.toList))
		assertRDDEquals(TestData.entityRDD(sc), entities)
	}
}
