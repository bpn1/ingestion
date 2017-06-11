package de.hpi.ingestion.textmining.models

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.{AliasTrieSearch, TestData => TextTestData}
import org.scalatest.{FlatSpec, Matchers}

class DFDataTest extends FlatSpec with Matchers with SharedSparkContext {

	"Tf-idf contexts for parsed Wikipedia articles with complete document frequencies" should "not be empty" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction

		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = TextTestData.articlesWithCompleteContexts()
		val numDocuments = articles.size
		val dfData = DFData(numDocuments, 2)

		val contexts = articles.map(_.context.map(dfData.idf))
		contexts should not be empty
		contexts.foreach(context => context should not be empty)

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	they should "be exactly these contexts" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction

		val testDocFreqFunction = TextTestData.docfreqStream("docfreq2") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = TextTestData.articlesWithCompleteContexts()
				.filter(_.title != "Streitberg (Brachttal)")
		val numDocuments = articles.size
		val dfData = DFData(numDocuments, 2)

		val contexts = articles.map(article => (article.title, article.context.map(dfData.idf)))
		contexts shouldEqual TestData.tfidfContextsSet()

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Tf-idf contexts for parsed Wikipedia articles with missing document frequencies" should
		"be exactly these contexts" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction

		val testDocFreqFunction = TextTestData.docfreqStream("docfreq2") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = TextTestData.articlesWithCompleteContexts()
			.filter(_.title != "Streitberg (Brachttal)")
		val numDocuments = articles.size
		val dfData = DFData(numDocuments, 2)

		val contexts = articles.map(article => (article.title, article.context.map(dfData.idf)))
		contexts shouldEqual TestData.tfidfContextsSet()

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Tf-Idf of link contexts" should "exist" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction

		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = TextTestData.articlesWithCompleteContexts()
		val numDocuments = articles.size
		val dfData = DFData(numDocuments, 2)

		val linkContextValues = articles.flatMap { article =>
			article.linkswithcontext.map(link => (link, link.context.map(dfData.idf)))
		}
		linkContextValues should not be empty

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	they should "be exactly these tf-Idf values (disregarding the contexts)" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction

		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = TextTestData.articlesWithCompleteContexts()
		val numDocuments = articles.size
		val dfData = DFData(numDocuments, 2)

		val linkContextValues = articles.flatMap { article =>
			article.linkswithcontext.map(link => (link.copy(context = Map()), link.context.map(dfData.idf)))
		}
		val expectedTfidf = TextTestData.linkContextsTfidf()
		linkContextValues shouldBe expectedTfidf

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	they should "compute the tf-Idf values for the linkswithcontext and trialiases contexts" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction

		val testDocFreqFunction = TextTestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val articles = TestData.articlesWithLinkAndAliasContexts()
		val numDocuments = articles.size
		val dfData = DFData(numDocuments, 2)

		val linkContextValues = articles.flatMap { article =>
			article.featureEntryLinks().map(link => (link.copy(context = Map()), link.context.map(dfData.idf)))
		}
		val expectedTfidf = TestData.linkContextsTfidfWithTrieAliases()
		linkContextValues shouldBe expectedTfidf

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Inverse document frequencies" should "contain as many tokens as document frequencies" in {
		val documentFrequencies = TextTestData.documentFrequenciesSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(df => (df.word, DFData.calculateIdf(df.count, numDocuments)))
		inverseDocumentFrequencies.size shouldEqual documentFrequencies.size
	}

	they should "be exactly these inverse document frequencies" in {
		val documentFrequencies = TextTestData.documentFrequenciesSet()
		val numDocuments = 4
		val inverseDocumentFrequencies = documentFrequencies
			.map(df => (df.word, DFData.calculateIdf(df.count, numDocuments)))
		inverseDocumentFrequencies shouldEqual TestData.inverseDocumentFrequenciesSet()
	}

	they should "be read and calculated from a file" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction

		val testDocFreqFunction = TextTestData.docfreqStream("smalldocfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction
		val idfMap = DFData.inverseDocumentFrequencies(4)
		val expectedIdfs = TestData.inverseDocumentFrequenciesSet().toMap
		idfMap shouldEqual expectedIdfs

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}

	"Idf map" should "be accessed" in {
		val idfMap = TestData.idfMap()
		val dfData = DFData(10, 0.1337, idfMap)
		idfMap.foreach { case (term, idf) =>
			idf shouldEqual dfData(term)
		}
		val noIdfTerms = List("abc", "def", "asd")
		noIdfTerms.foreach { term =>
			dfData(term) shouldEqual dfData.defaultIdf
		}
	}
}
