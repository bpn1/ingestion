package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class TextNERTest extends FlatSpec with SharedSparkContext with Matchers {
	"Found named entities" should "not be empty" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		texts
			.map(text => TextNER.findNamedEntities(text, classifier))
			.foreach(_ should not be empty)
	}

	they should "have offsets that are consistent with their token" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		texts
			.map(text => TextNER.findNamedEntities(text, classifier))
			.foreach { namedEntities =>
				namedEntities.foreach { namedEntity =>
					namedEntity.token.length shouldEqual namedEntity.endOffset - namedEntity.beginOffset
				}
			}
	}

	they should "have offsets that are consistent with the text" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		texts
			.map(text => (text, TextNER.findNamedEntities(text, classifier)))
			.foreach { case (text, namedEntities) =>
				namedEntities.foreach { namedEntity =>
					namedEntity.token.length shouldEqual namedEntity.endOffset - namedEntity.beginOffset
					val start = namedEntity.beginOffset
					val end = namedEntity.beginOffset + namedEntity.token.length
					val substring = text.substring(start, end)
					substring shouldEqual namedEntity.token
				}
			}
	}

	they should "be exactly these named entities" in {
		val texts = TestData.parsedWikipediaSet()
			.map(_.getText())
		val classifier = TestData.randomForestModel()
		val namedEntities = texts
			.map(text => TextNER.findNamedEntities(text, classifier))
		namedEntities shouldEqual TestData.namedEntitiesList()
	}
}
