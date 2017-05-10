package de.hpi.ingestion.dataimport.wikipedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class WikipediaImportTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {

	"XML" should "be parsed" in {
		val entries = TestData.pageXML.map(WikipediaImport.parseXML)
		val expected = TestData.wikipediaEntries
		entries shouldEqual expected
	}

	"Wikipedia" should "be parsed" in {
		val xmlInput = List(sc.parallelize(TestData.pageXML)).toAnyRDD()
		val entries = WikipediaImport.run(xmlInput, sc).fromAnyRDD[WikipediaEntry]().head
		val expected = sc.parallelize(TestData.wikipediaEntries)
		assertRDDEquals(entries, expected)
	}
}
