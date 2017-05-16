package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class TextNERTest extends FlatSpec with SharedSparkContext with Matchers {
	"Found named entities" should "not be empty" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		texts
			.map(text => TextNER.findNamedEntities(text))
			.foreach(_ should not be empty)
	}
}
