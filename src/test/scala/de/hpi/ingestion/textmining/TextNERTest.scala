package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.Link
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class TextNERTest extends FlatSpec with SharedSparkContext with Matchers {
	//	"Link candidates" should "not be empty" in {
	//		val texts = TestData.parsedWikipediaSet()
	//			.map(_.getText())
	//		val tokenizer = IngestionTokenizer(true, false)
	//		texts
	//			.map(text => TextNER.generateLinkCandiates(text, tokenizer))
	//			.foreach(_ should not be empty)
	//	}
	//
	//
	//	they should "contain these link candidates" in {
	//		val texts = TestData.parsedWikipediaSet()
	//			.map(_.getText())
	//		val classifier = TestData.randomForestModel()
	//		val tokenizer = IngestionTokenizer(true, false)
	//		val linkCandidates = texts.map(text => TextNER.generateLinkCandiates(text, tokenizer))
	//		(linkCandidates, TestData.linkCandidates())
	//			.zipped
	//			.foreach((actual, expected) => actual should contain allElementsOf expected)
	//	}

	"Found named entities" should "be empty" in {
		val articles = sc.parallelize(TestData.articlesWithCompleteContexts().toList)
		val documentFrequencies = sc.parallelize(TestData.filteredStemmedDocumentFrequenciesSet().toList)
		val aliases = sc.parallelize(TestData.aliasesWithExistingPagesSet().toList)
		val input = List(articles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD() ++ List(aliases).toAnyRDD()
		val namedEntities = TextNER
			.run(input, sc)
			.fromAnyRDD[(String, Link)]()
			.head
			.collect
		namedEntities shouldBe empty
	}
}
