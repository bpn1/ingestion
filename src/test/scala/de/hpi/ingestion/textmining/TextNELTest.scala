package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.Link
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class TextNELTest extends FlatSpec with SharedSparkContext with Matchers {
	"Found named entities" should "be empty" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldCCCSettings = CosineContextComparator.settings
		val oldSettings = TextNEL.settings
		CosineContextComparator.parseConfig()
		TextNEL.parseConfig()
		val testDocFreqFunction = TestData.docfreqStream("docfreq") _
		AliasTrieSearch.trieStreamFunction = testDocFreqFunction

		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD() ++ List(aliases).toAnyRDD()

		TextNEL.modelFunction = TestData.loadRandomForestModel _
		val namedEntities = TextNEL
			.run(input, sc)
			.fromAnyRDD[Link]()
			.head
			.collect
		namedEntities shouldBe empty

		TextNEL.settings = oldSettings
		CosineContextComparator.settings = oldCCCSettings
		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
	}
}
