/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.tokenizer

import de.hpi.ingestion.textmining.TestData
import org.scalatest.{FlatSpec, Matchers}

class IngestionTokenizerTest extends FlatSpec with Matchers {

	"Input String" should "be processed" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true)
		val tokens = TestData.sentencesList()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedAndFilteredSentences()
		tokens shouldEqual expectedTokens
	}

	it should "keep stopwords" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, false, true)
		val tokens = TestData.sentencesList()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedTokenizedSentences()
		tokens shouldEqual expectedTokens

	}

	it should "not be stemmed" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer, true, false)
		val tokens = TestData.sentencesList()
			.map(tokenizer.process)
		val expectedTokens = TestData.filteredTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	it should "only be tokenized" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.sentencesList()
			.map(tokenizer.process)
		val expectedTokens = TestData.tokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	"Input tokens" should "be processed" in {
		val tokenizer = IngestionTokenizer()
		val tokens = TestData.tokenizedSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedAndFilteredSentences()
		tokens shouldEqual expectedTokens
	}

	they should "keep stopwords" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, false, true)
		val tokens = TestData.tokenizedSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.stemmedTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	they should "not be stemmed" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, true)
		val tokens = TestData.tokenizedSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.filteredTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	they should "remain unchanged" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.tokenizedSentences()
			.map(tokenizer.process)
		val expectedTokens = TestData.tokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	"Only tokenize" should "only tokenize the input" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokens = TestData.sentencesList()
			.map(tokenizer.onlyTokenize)
		val expectedTokens = TestData.tokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	"Reverse" should "revert the input tokens" in {
		val tokenizer = IngestionTokenizer(new CleanCoreNLPTokenizer)
		val tokenizer2 = IngestionTokenizer(new CoreNLPTokenizer)
		val sentences = TestData.tokenizedSentences()
			.map(tokenizer.reverse)
		val sentences2 = TestData.tokenizedSentences()
			.map(tokenizer2.reverse)
		val expectedSentences = TestData.reversedSentences()
		sentences shouldEqual expectedSentences
		sentences2 shouldEqual expectedSentences
	}

	"String arguments apply" should "parse tokenizer option" in {
		var tokenizer = IngestionTokenizer(List("WhitespaceTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[WhitespaceTokenizer]
		tokenizer = IngestionTokenizer(List("CleanWhitespaceTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CleanWhitespaceTokenizer]
		tokenizer = IngestionTokenizer(List("CleanCoreNLPTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CleanCoreNLPTokenizer]
		tokenizer = IngestionTokenizer(List("CoreNLPTokenizer"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CoreNLPTokenizer]
		tokenizer = IngestionTokenizer(List("Broken"))
		tokenizer.tokenizer.getClass shouldEqual classOf[CoreNLPTokenizer]
		tokenizer = IngestionTokenizer(List[String]())
		tokenizer.tokenizer.getClass shouldEqual classOf[CoreNLPTokenizer]
	}

	it should "parse stopword filtering option" in {
		val tokenizerName = "WhitespaceTokenizer"
		var tokenizer = IngestionTokenizer(List[String]())
		tokenizer.removeStopwords shouldBe true
		tokenizer = IngestionTokenizer(List(tokenizerName))
		tokenizer.removeStopwords shouldBe true
		tokenizer = IngestionTokenizer(List(tokenizerName, "true"))
		tokenizer.removeStopwords shouldBe true
		tokenizer = IngestionTokenizer(List(tokenizerName, "false"))
		tokenizer.removeStopwords shouldBe false
		tokenizer = IngestionTokenizer(List(tokenizerName, "broken"))
		tokenizer.removeStopwords shouldBe false
	}

	it should "parse stemming option" in {
		val tokenizerName = "WhitespaceTokenizer"
		var tokenizer = IngestionTokenizer(List[String]())
		tokenizer.stem shouldBe true
		tokenizer = IngestionTokenizer(List(tokenizerName))
		tokenizer.stem shouldBe true
		tokenizer = IngestionTokenizer(List(tokenizerName, "true", "true"))
		tokenizer.stem shouldBe true
		tokenizer = IngestionTokenizer(List(tokenizerName, "true", "false"))
		tokenizer.stem shouldBe false
		tokenizer = IngestionTokenizer(List(tokenizerName, "true", "broken"))
		tokenizer.stem shouldBe false
	}

	"Tokens with offset" should "not be empty" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, false, false)
		TestData.sentencesList()
			.map(tokenizer.processWithOffsets)
			.foreach(_ should not be empty)
	}

	they should "have offsets that are consistent with their token" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, false, false)
		TestData.sentencesList()
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
		TestData.sentencesList()
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

	they should "not contain stopwords" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, true, false)
		val tokens = TestData.sentencesList()
			.map(tokenizer.processWithOffsets)
			.map(_.map(_.token))
		val expectedTokens = TestData.filteredUncleanTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	they should "be stemmed" in {
		val tokenizer = IngestionTokenizer(new CoreNLPTokenizer, true, true)
		val tokens = TestData.sentencesList()
			.map(tokenizer.processWithOffsets)
			.map(_.map(_.token))
		val expectedTokens = TestData.stemmedAndFilteredUncleanTokenizedSentences()
		tokens shouldEqual expectedTokens
	}

	they should "only be computed if the tokenizer supports them" in {
		val tokenizer = new WhitespaceTokenizer
		val tokens = tokenizer.tokenizeWithOffsets("This is a test String.")
		tokens shouldBe empty
	}

	"Cleaned tokenized sentences" should "contain multiple tokens" in {
		val tokenizer = IngestionTokenizer(new CleanWhitespaceTokenizer, false, false)
		TestData.sentencesList()
			.map(tokenizer.process)
			.foreach(tokens => tokens.length should be > 1)
	}

	they should "be exactly these token lists" in {
		val tokenizer = IngestionTokenizer(new CleanWhitespaceTokenizer, false, false)
		val tokenizedSentences = TestData.sentencesList()
			.map(tokenizer.process)
		tokenizedSentences shouldEqual TestData.tokenizedSentencesWithoutSpecialCharacters()
	}

	"Clean Whitespace Tokenizer" should "reverse token lists" in {
		val tokenizer = new CleanWhitespaceTokenizer
		val reversedTexts = TestData.tokenizedSentencesWithoutSpecialCharacters().map(tokenizer.reverse)
		val expectedTexts = TestData.cleanedReversedSentences()
		reversedTexts shouldEqual expectedTexts
	}

	it should "strip tokens of bad tokens" in {
		val tokenizer = new CleanWhitespaceTokenizer
		val badCharacters = "().!?,;:'`\"„“"
		val tokens = List("", "a.", ".", ".a", "a.a", "...(.a").map(tokenizer.stripAll(_, badCharacters))
		val expectedTokens = List("", "a", "", "a", "a.a", "a")
		tokens shouldEqual expectedTokens
	}

	"Whitespace Tokenizer" should "tokenize text" in {
		val tokenizer = new WhitespaceTokenizer
		val tokenList = TestData.sentencesList().map(tokenizer.tokenize)
		val expected = TestData.uncleanTokenizedSentences()
		tokenList shouldEqual expected
	}

	it should "reverse tokens into text" in {
		val tokenizer = new WhitespaceTokenizer
		val sentences = TestData.uncleanTokenizedSentences().map(tokenizer.reverse)
		val expected = TestData.sentencesList()
		sentences shouldEqual expected
	}

	"Stopword file header" should "not be parsed" in {
		val stopwords = IngestionTokenizer(new CleanCoreNLPTokenizer, true, true).stopwords
		stopwords.foreach(_.head should not equal ';')
	}

	def isSpecialCharacter(text: String, beginOffset: Int, endOffset: Int): Boolean = {
		val length = endOffset - beginOffset
		val character = text.charAt(beginOffset)
		length == 1 && !character.isLetter && !character.isDigit
	}
}
