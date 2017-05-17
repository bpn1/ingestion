package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.models.LinkCandidate
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.scalatest.{FlatSpec, Matchers}

class TextNERTest extends FlatSpec with SharedSparkContext with Matchers {
	"Link candidates" should "not be empty" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val tokenizer = IngestionTokenizer(true, false)
		texts
			.map(text => TextNER.generateLinkCandiates(text, tokenizer))
			.foreach(_ should not be empty)
	}

	they should "have offsets that are consistent with their token" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		val tokenizer = IngestionTokenizer(true, false)
		texts
			.map(text => (text, TextNER.generateLinkCandiates(text, tokenizer)))
			.foreach { case (text, linkCandidates) =>
				linkCandidates.foreach { case LinkCandidate(offsetToken, linkCandidate) =>
					if(!isSpecialCharacter(text, offsetToken.beginOffset, offsetToken.endOffset)) {
						offsetToken.token.length shouldEqual offsetToken.endOffset - offsetToken.beginOffset
					}
				}
			}
	}

	they should "have offsets that are consistent with the text" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		val tokenizer = IngestionTokenizer(true, false)
		texts
			.map(text => (text, TextNER.generateLinkCandiates(text, tokenizer)))
			.foreach { case (text, linkCandidates) =>
				linkCandidates.foreach { case LinkCandidate(offsetToken, linkCandidate) =>
					if(!isSpecialCharacter(text, offsetToken.beginOffset, offsetToken.endOffset)) {
						offsetToken.token.length shouldEqual offsetToken.endOffset - offsetToken.beginOffset
						val start = offsetToken.beginOffset
						val end = offsetToken.beginOffset + offsetToken.token.length
						val substring = text.substring(start, end)
						substring shouldEqual offsetToken.token
					}
				}
			}
	}

	they should "contain these link candidates" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		val tokenizer = IngestionTokenizer(true, false)
		val linkCandidates = texts.map(text => TextNER.generateLinkCandiates(text, tokenizer))
		(linkCandidates, TestData.linkCandidates())
			.zipped
			.foreach((actual, expected) => actual should contain allElementsOf expected)
	}

	def isSpecialCharacter(text: String, beginOffset: Int, endOffset: Int): Boolean = {
		val length = endOffset - beginOffset
		val character = text.charAt(beginOffset)
		length == 1 && !character.isLetter && !character.isDigit
	}
}
