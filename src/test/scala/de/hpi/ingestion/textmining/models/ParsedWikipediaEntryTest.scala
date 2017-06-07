package de.hpi.ingestion.textmining.models

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class ParsedWikipediaEntryTest extends FlatSpec with SharedSparkContext with Matchers {
	"All links" should "be exactly these links" in {
		val entry = TestData.parsedEntryWithDifferentLinkTypes()
		entry.allLinks() shouldEqual TestData.allLinksListFromEntryList()
	}

	"Filtered links" should "be exactly these links" in {
		val entry = TestData.parsedEntryWithDifferentLinkTypes()
		entry.filterLinks(link => link.alias == link.page)
		entry shouldEqual TestData.parsedEntryWithFilteredLinks()
	}

	"Textlinks with context" should "be exactly these links" in {
		val entry = TestData.parsedEntryWithContext()
		val textlinks = entry.textlinkContexts()
		val expected = TestData.textLinksWithContext()
		textlinks shouldEqual expected
	}

	"Extendedlinks with context" should "be exactly these links" in {
		val entry = TestData.parsedEntryWithContext()
		val extendedlinks = entry.extendedlinkContexts()
		val expected = TestData.extendedLinksWithContext()
		extendedlinks shouldEqual expected
	}

	"Reduced Extended Links" should "be exactly these Links" in {
		val entry = TestData.extendedLinksParsedWikipediaEntry()
		entry.extendedlinks(noTextLinks = false) shouldEqual TestData.edgeCaseExtendedLinksToLinks()
	}

	"Filtered Extended Links" should "not contain links that are colliding with textlinks" in {
		val entry = TestData.parsedWikipediaEntryWithLinkCollisions()
		entry.extendedlinks().size shouldEqual 4
	}

	"All Links" should "be exactly these links" in {
		val entry = TestData.parsedWikipediaEntryWithLinkCollisions()
		entry.allLinks() shouldEqual TestData.linksWithoutCollisions()
	}

	"Filtered Extended Links" should "not contain any textlinks" in {
		val entry = TestData.extendedLinksParsedWikipediaEntry()
		entry.extendedlinks() should not contain TestData.extendedLinksParsedWikipediaEntry().textlinks
	}
}
