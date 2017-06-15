package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class ContextPerformanceProfilerTest extends FlatSpec with SharedSparkContext with Matchers {
	"Idf vectors" should "not be empty" in {
		val documents = sc.parallelize(TestData.tokenizedDocumentsList())
		val idfVectors = ContextPerformanceProfiler.sparkMLlibContextExtraction(documents, sc)
		    	.collect
		idfVectors should not be empty
		idfVectors.foreach(_.toArray should not be empty)
	}

	"Statistics" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(aliases).toAnyRDD()
		val statistics = ContextPerformanceProfiler.run(input, sc)
		statistics should not be empty
	}
}
