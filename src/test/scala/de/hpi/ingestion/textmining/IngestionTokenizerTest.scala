package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}

class IngestionTokenizerTest extends FlatSpec with Matchers {

	"Input String" should "be processed" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val tokens = TestData.testSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedAndFilteredSentences()
		tokens shouldEqual expectedTokens
	}

	it should "keep stopwords" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, false, true)
		val tokens = TestData.testSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedTokenizedSentences()
		tokens shouldEqual expectedTokens

	}

	it should "not be stemmed" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, false)
		val tokens = TestData.testSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.filteredTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	it should "only be tokenized" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.testSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.tokenizedTestSentences()
		tokens shouldEqual expectedTokens
	}

	"Input tokens" should "be processed" in {
		val tokenizer = IngestionTokenizer()
		val tokens = TestData.tokenizedTestSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedAndFilteredSentences()
		tokens shouldEqual expectedTokens
	}

	they should "keep stopwords" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, false, true)
		val tokens = TestData.tokenizedTestSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	they should "not be stemmed" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, true)
		val tokens = TestData.tokenizedTestSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.filteredTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	they should "remain unchanged" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.tokenizedTestSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.tokenizedTestSentences()
		tokens shouldEqual expectedTokens
	}

	"Only tokenize" should "only tokenize the input" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.testSentences()
			.map(tokenizer.onlyTokenize)
		val expectedTokens = TestData.tokenizedTestSentences()
		tokens shouldEqual expectedTokens
	}

	"Reverse" should "revert the input tokens" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokenizer2 = IngestionTokenizer(new CoreNLPTokenizer)
		val sentences = TestData.tokenizedTestSentences()
			.map(tokenizer.reverse)
		val sentences2 = TestData.tokenizedTestSentences()
			.map(tokenizer2.reverse)
		val expectedSentences = TestData.reversedSentences()
		sentences shouldEqual expectedSentences
		sentences2 shouldEqual expectedSentences
	}

	"String arguments apply" should "parse tokenizer option" in {
		var tokenizer = IngestionTokenizer(Array("WhitespaceTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[WhitespaceTokenizer]
		tokenizer = IngestionTokenizer(Array("CleanWhitespaceTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CleanWhitespaceTokenizer]
		tokenizer = IngestionTokenizer(Array("CleanCoreNLPTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CleanCoreNLPTokenizer]
		tokenizer = IngestionTokenizer(Array("CoreNLPTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CoreNLPTokenizer]
		tokenizer = IngestionTokenizer(Array("Broken"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CoreNLPTokenizer]
		tokenizer = IngestionTokenizer(Array[String]())
		tokenizer.tokenizer.getClass shouldEqual classOf[CoreNLPTokenizer]
	}

	it should "parse stopword filtering option" in {
		val tokenizerName = "WhitespaceTokenizer"
		var tokenizer = IngestionTokenizer(Array[String]())
		tokenizer.removeStopwords shouldBe true
		tokenizer = IngestionTokenizer(Array(tokenizerName))
		tokenizer.removeStopwords shouldBe true
		tokenizer = IngestionTokenizer(Array(tokenizerName, "true"))
		tokenizer.removeStopwords shouldBe true
		tokenizer = IngestionTokenizer(Array(tokenizerName, "false"))
		tokenizer.removeStopwords shouldBe false
		tokenizer = IngestionTokenizer(Array(tokenizerName, "broken"))
		tokenizer.removeStopwords shouldBe false
	}

	it should "parse stemming option" in {
		val tokenizerName = "WhitespaceTokenizer"
		var tokenizer = IngestionTokenizer(Array[String]())
		tokenizer.stem shouldBe true
		tokenizer = IngestionTokenizer(Array(tokenizerName))
		tokenizer.stem shouldBe true
		tokenizer = IngestionTokenizer(Array(tokenizerName, "true", "true"))
		tokenizer.stem shouldBe true
		tokenizer = IngestionTokenizer(Array(tokenizerName, "true", "false"))
		tokenizer.stem shouldBe false
		tokenizer = IngestionTokenizer(Array(tokenizerName, "true", "broken"))
		tokenizer.stem shouldBe false
	}

	"Tokens with offset" should "not be empty" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, false, false)
		TestData.testSentences()
			.map(tokenizer.processWithOffsets)
			.foreach(_ should not be empty)
	}

	they should "have offsets that are consistent with their token" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, false, false)
		TestData.testSentences()
			.map(sentence => (sentence, tokenizer.processWithOffsets(sentence)))
			.foreach { case (sentence, tokensWithOffset) =>
				tokensWithOffset.foreach { tokenWithOffset =>
					if(!isSpecialCharacter(sentence, tokenWithOffset.beginOffset, tokenWithOffset.endOffset)) {
						tokenWithOffset.token.length shouldEqual tokenWithOffset.endOffset - tokenWithOffset.beginOffset
					}
				}
			}
	}

	they should "have offsets that are consistent with the text" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, false, false)
		TestData.testSentences()
			.map(sentence => (sentence, tokenizer.processWithOffsets(sentence)))
			.foreach { case (sentence, tokensWithOffset) =>
				tokensWithOffset.foreach { tokenWithOffset =>
					if(!isSpecialCharacter(sentence, tokenWithOffset.beginOffset, tokenWithOffset.endOffset)) {
						val start = tokenWithOffset.beginOffset
						val end = tokenWithOffset.beginOffset + tokenWithOffset.token.length
						val substring = sentence.substring(start, end)
						substring shouldEqual tokenWithOffset.token
					}
				}
			}
	}

	"Cleaned tokenized sentences" should "contain multiple tokens" in {
		val tokenizer = IngestionTokenizer(new CleanWhitespaceTokenizer, false, false)
		TestData.testSentences()
			.map(tokenizer.process)
			.foreach(tokens => tokens.length should be > 1)
	}

	they should "be exactly these token lists" in {
		val tokenizer = IngestionTokenizer(new CleanWhitespaceTokenizer, false, false)
		val tokenizedSentences = TestData.testSentences()
			.map(tokenizer.process)
		tokenizedSentences shouldEqual TestData.tokenizedTestSentencesWithoutSpecialCharacters()
	}

	def isSpecialCharacter(text: String, beginOffset: Int, endOffset: Int): Boolean = {
		val length = endOffset - beginOffset
		val character = text.charAt(beginOffset)
		length == 1 && !character.isLetter && !character.isDigit
	}
}
