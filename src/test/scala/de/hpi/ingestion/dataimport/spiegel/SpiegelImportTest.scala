package de.hpi.ingestion.dataimport.spiegel

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.TrieAliasArticle
import de.hpi.ingestion.textmining.{AliasTrieSearch, TestData => TextTestData}

class SpiegelImportTest extends FlatSpec with Matchers with SharedSparkContext {

	"Spiegel articles" should "be parsed" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()

		val testTrieStreamFunction = TextTestData.fullTrieStream("/spiegel/triealiases") _
		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction
		val input = List(sc.parallelize(TestData.spiegelFile())).toAnyRDD()
		val articles = SpiegelImport
			.run(input, sc)
			.fromAnyRDD[TrieAliasArticle]()
			.head
			.collect
			.toSet
		val expectedArticles = TestData.parsedArticles().toSet
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
}
