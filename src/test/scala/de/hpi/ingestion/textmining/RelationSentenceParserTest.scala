package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import org.scalatest.{FlatSpec, Matchers}

class RelationSentenceParserTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Wikipedia text" should "be split into Sentences" in {
		val parsedEntry = TestData.bigLinkExtenderParsedEntry()
		val tokenizer = new CoreNLPSentenceTokenizer
		val sentences = RelationSentenceParser.entryToSentencesWithEntitites(parsedEntry, tokenizer)
		sentences shouldBe TestData.sentenceList()
	}
}
