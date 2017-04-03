package de.hpi.ingestion.textmining

import org.scalatest.FlatSpec

class CleanWhitespaceTokenizerTest extends FlatSpec with PrettyTester {
	"Cleaned tokenized sentences" should "contain multiple tokens" in {
		val tokenizer = new CleanWhitespaceTokenizer
		TestData.testSentences()
			.map(tokenizer.tokenize)
			.foreach(tokens => assert(tokens.length > 1))
	}

	they should "be exactly these token lists" in {
		val tokenizer = new CleanWhitespaceTokenizer
		val tokenizedSentences = TestData.testSentences()
			.map(tokenizer.tokenize)
		assert(areListsEqual(tokenizedSentences, TestData.tokenizedTestSentences()))
	}
}
