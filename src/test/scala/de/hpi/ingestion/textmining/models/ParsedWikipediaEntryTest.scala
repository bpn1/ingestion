package de.hpi.ingestion.textmining.models

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class ParsedWikipediaEntryTest extends FlatSpec with SharedSparkContext with Matchers {
	"All links" should "be exactly these links" in {
		val entry = TestData.parsedEntryWithDifferentLinkTypes()
		entry.allLinks() shouldEqual TestData.allLinksListFromEntryList()
	}

	"Reduced links" should "be exactly these links" in {
		val entry = TestData.parsedEntryWithDifferentAndReducedLinkTypes()
		entry.reducedLinks() shouldEqual TestData.allReducedLinks()
	}

	"Filtered links" should "be exactly these links" in {
		val entry = TestData.parsedEntryWithDifferentLinkTypes()
		entry.filterLinks(link => link.alias == link.page)
		entry shouldEqual TestData.parsedEntryWithFilteredLinks()
	}

	they should "be written to the reduced columns" in {
		val entry = TestData.parsedEntryWithDifferentLinkTypes()
		entry.filterLinksToReduced(link => link.alias == link.page)
		entry shouldEqual TestData.parsedEntryWithReducedLinks()
	}

	"Filtered Extended Links" should "be exactly these Links" in {
		val entry = TestData.extendedLinksParsedWikipediaEntry()
		entry.extendedlinks() shouldEqual TestData.edgeCaseExtendedLinksToLinks()
	}
}
