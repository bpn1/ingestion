package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.textmining.TestData.parsedWikipediaWithTextsSet
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.TrieAliasArticle
import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class WikipediaReductionTest extends FlatSpec with SharedSparkContext with Matchers {
	"Reduced Wikipedia articles" should "not be empty" in {
		val articles = sc.parallelize(parsedWikipediaWithTextsSet().toList)
		val input = List(articles).toAnyRDD()
		val reducedArticles = WikipediaReduction.run(input, sc)
			.fromAnyRDD[TrieAliasArticle]()
			.head
		reducedArticles should not be empty
	}

	they should "be exactly these articles" in {
		val articles = sc.parallelize(parsedWikipediaWithTextsSet().toList)
		val input = List(articles).toAnyRDD()
		val reducedArticles = WikipediaReduction.run(input, sc)
			.fromAnyRDD[TrieAliasArticle]()
			.head
			.collect
			.toSet
		reducedArticles shouldEqual TestData.reducedWikipediaArticles()
	}
}
