package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._


class DBpediaRelationParserTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

	"Translation triple" should "be parsed into map from english to german wikipedia titles" in {
		val ttl = sc.parallelize(TestData.interlanguageLinksEn())
		val prefixesList = TestData.prefixesList
		DBpediaRelationParser.getGermanLabels(ttl, prefixesList) shouldEqual TestData.germanLabels()
	}

	"The parsed relations" should "be exactly these" in {
		val labels = sc.parallelize(TestData.interlanguageLinksEn())
		val dbpedia = sc.parallelize(TestData.dbpediaRawRelations())
		val input = List(dbpedia).toAnyRDD() ++ List(labels).toAnyRDD()
		val relations = DBpediaRelationParser.run(input, sc)
			.fromAnyRDD[Relation]()
			.head
			.collect
			.toList
		relations shouldEqual TestData.dbpediaParsedRelations()
	}
}
