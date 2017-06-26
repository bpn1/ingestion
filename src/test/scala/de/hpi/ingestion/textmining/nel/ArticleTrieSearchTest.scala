package de.hpi.ingestion.textmining.nel

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.TrieAliasArticle
import de.hpi.ingestion.textmining.{AliasTrieSearch, TestData => TextTestData}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.scalatest.{FlatSpec, Matchers}

class ArticleTrieSearchTest extends FlatSpec with Matchers with SharedSparkContext {
	"Enriched articles" should "be exactly these articles" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()

		val testTrieStreamFunction = TextTestData.fullTrieStream("/spiegel/triealiases") _
		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
		val input = List(sc.parallelize(TestData.aliasSearchArticles())).toAnyRDD()
		val articles = ArticleTrieSearch.run(input, sc).fromAnyRDD[TrieAliasArticle]().head.collect.toSet
		val expectedArticles = TestData.foundTrieAliases()
		articles shouldEqual expectedArticles

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
		AliasTrieSearch.settings = oldSettings
	}

	"Found aliases" should "be exactly these aliases" in {
		val tokenizer = IngestionTokenizer(false, false)
		val trie = TextTestData.dataTrie(tokenizer, "/spiegel/triealiases")
		val articles = TestData.aliasSearchArticles()
			.map(ArticleTrieSearch.findAliases(_, tokenizer, trie))
		val expectedArticles = TestData.foundAliasArticles()
		articles shouldEqual expectedArticles
	}
}
