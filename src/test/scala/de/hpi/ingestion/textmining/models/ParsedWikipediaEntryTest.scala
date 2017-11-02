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
        entry.reduceLinks(link => link.alias == link.page)
        entry shouldEqual TestData.parsedEntryWithReducedLinks()
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
