package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}

class IngestionTokenizerTest extends FlatSpec with Matchers {

	"Input String" should "be processed" in {
		val tokenizer = new IngestionTokenizer(new CoreNLPTokenizer, true, true)
		assert(false)
	}

	it should "keep stopwords" in {
		val tokenizer = new IngestionTokenizer(new CoreNLPTokenizer, false, true)
		assert(false)
	}

	it should "not be stemmed" in {
		val tokenizer = new IngestionTokenizer(new CoreNLPTokenizer, true)
		assert(false)
	}

	it should "only be tokenized" in {
		val tokenizer = new IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.testSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.tokenizedTestSentences()
		tokens shouldEqual expectedTokens
	}

	"Input tokens" should "be processed" in {
		val tokenizer = new IngestionTokenizer(new CoreNLPTokenizer, true, true)
		assert(false)
	}

	they should "keep stopwords" in {
		val tokenizer = new IngestionTokenizer(new CoreNLPTokenizer, false, true)
		assert(false)
	}

	they should "not be stemmed" in {
		val tokenizer = new IngestionTokenizer(new CoreNLPTokenizer, true)
		assert(false)
	}

	they should "remain unchanged" in {
		val tokenizer = new IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.tokenizedTestSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.tokenizedTestSentences()
		tokens shouldEqual expectedTokens
	}

	"Only tokenize" should "only tokenize the input" in {
		val tokenizer = new IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.testSentences()
			.map(tokenizer.onlyTokenize)
		val expectedTokens = TestData.tokenizedTestSentences()
		tokens shouldEqual expectedTokens
	}

	"Reverse" should "revert the input tokens" in {
		val tokenizer = new IngestionTokenizer(new CleanCoreNLPTokenizer)
		val sentences = TestData.tokenizedTestSentences()
			.map(tokenizer.reverse)
		val expectedSentences = TestData.reversedSentences()
		sentences shouldEqual expectedSentences
	}
}
