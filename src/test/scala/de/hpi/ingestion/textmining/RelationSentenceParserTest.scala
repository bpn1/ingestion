package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.Sentence
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.scalatest.{FlatSpec, Matchers}

class RelationSentenceParserTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Wikipedia text" should "be split into exactly these Sentences with these entities" in {
		val parsedEntry = TestData.bigLinkExtenderParsedEntry()
		val sentenceTokenizer = IngestionTokenizer(Array("SentenceTokenizer", "false", "false"))
		val tokenizer = IngestionTokenizer(Array("CleanWhitespaceTokenizer", "false", "false"))
		val sentences = RelationSentenceParser.entryToSentencesWithEntities(parsedEntry, sentenceTokenizer, tokenizer)
		sentences shouldEqual TestData.sentenceList()
	}

	"Wikipedia articles" should "be split into exactly these Sentences with these entities" in {
		val entries = sc.parallelize(
			(Set(TestData.bigLinkExtenderParsedEntry()) ++ TestData.linkExtenderExtendedParsedEntry()).toList
		)
		val input = List(entries).toAnyRDD()
		val sentences = RelationSentenceParser.run(input, sc)
			.fromAnyRDD[Sentence]()
			.head
			.collect
			.toList
		val expectedSentences = TestData.sentenceList() ++ TestData.alternativeSentenceList()
		sentences shouldEqual expectedSentences
	}
}
