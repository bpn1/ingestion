package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.scalatest.{FlatSpec, Matchers}

class TextNERTest extends FlatSpec with SharedSparkContext with Matchers {
	"Data points" should "not be empty" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val tokenizer = IngestionTokenizer(true, true)
		texts
			.map(text => TextNER.generateDataPoints(text, tokenizer))
			.foreach(_ should not be empty)
	}

	they should "have offsets that are consistent with their token" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		val tokenizer = IngestionTokenizer(true, true)
		texts
			.map(text => TextNER.generateDataPoints(text, tokenizer))
			.foreach { dataPoints =>
				dataPoints.foreach { case (offsetToken, labeledPoint) =>
					offsetToken.token.length shouldEqual offsetToken.endOffset - offsetToken.beginOffset
				}
			}
	}

	they should "have offsets that are consistent with the text" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		val tokenizer = IngestionTokenizer(true, true)
		texts
			.map(text => (text, TextNER.generateDataPoints(text, tokenizer)))
			.foreach { case (text, dataPoints) =>
				dataPoints.foreach { case (offsetToken, labeledPoint) =>
					offsetToken.token.length shouldEqual offsetToken.endOffset - offsetToken.beginOffset
					val start = offsetToken.beginOffset
					val end = offsetToken.beginOffset + offsetToken.token.length
					val substring = text.substring(start, end)
					substring shouldEqual offsetToken.token
				}
			}
	}
}
