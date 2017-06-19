package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class DBpediaRelationParserTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

	"Translation triple" should "be parsed into map from english to german wikipedia titles" in {
		val ttl = sc.parallelize(TestData.interlanguageLinksEn())
		DBpediaRelationParser.getGermanLabels(ttl) shouldEqual TestData.germanLabels()
	}

}
