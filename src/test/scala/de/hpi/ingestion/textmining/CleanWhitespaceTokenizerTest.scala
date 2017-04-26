package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}

class CleanWhitespaceTokenizerTest extends FlatSpec with Matchers {
	"Cleaned tokenized sentences" should "contain multiple tokens" in {
		val tokenizer = new CleanWhitespaceTokenizer
		TestData.testSentences()
			.map(tokenizer.tokenize)
			.foreach(tokens => tokens.length should be > 1)
	}

	they should "be exactly these token lists" in {
		val tokenizer = new CleanWhitespaceTokenizer
		val tokenizedSentences = TestData.testSentences()
			.map(tokenizer.tokenize)
		tokenizedSentences shouldEqual TestData.tokenizedTestSentences()
	}
}
