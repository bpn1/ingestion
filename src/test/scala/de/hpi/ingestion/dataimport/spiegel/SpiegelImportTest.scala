package de.hpi.ingestion.dataimport.spiegel

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import de.hpi.ingestion.textmining.{AliasTrieSearch, TestData => TextTestData}

class SpiegelImportTest extends FlatSpec with Matchers with SharedSparkContext {

	"Spiegel articles" should "be parsed" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()

		val testTrieStreamFunction = TextTestData.fullTrieStream("/spiegel/triealiases") _
		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
		val input = List(sc.parallelize(TestData.spiegelFile())).toAnyRDD()
		val articles = SpiegelImport.run(input, sc).fromAnyRDD[SpiegelArticle]().head.collect.toSet
		val expectedArticles = TestData.parsedAndSearchedArticles().toSet
		articles shouldEqual expectedArticles

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
		AliasTrieSearch.settings = oldSettings
	}

	"Article values" should "be extracted" in {
		val articles = TestData.spiegelJson().map(SpiegelImport.fillEntityValues)
		val expectedArticles = TestData.parsedArticles()
		articles shouldEqual expectedArticles
	}

	"Article text" should "be extracted" in {
		val extractedContents = TestData.spiegelPages().map(SpiegelImport.extractArticleText)
		val expectedContents = TestData.pageTexts()
		extractedContents shouldEqual expectedContents
	}

	"Found aliases" should "be exactly these aliases" in {
		val tokenizer = IngestionTokenizer(false, false)
		val trie = TextTestData.dataTrie(tokenizer, "/spiegel/triealiases")
		val articles = TestData.aliasSearchArticles()
		    .map(SpiegelImport.findAliases(_, tokenizer, trie))
		val expectedArticles = TestData.foundAliasArticles()
		articles shouldEqual expectedArticles
	}

}
