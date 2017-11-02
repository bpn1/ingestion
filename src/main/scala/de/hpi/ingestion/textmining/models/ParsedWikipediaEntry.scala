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

import scala.collection.mutable.ListBuffer

/**
  * Case class representing a Parsed Wikipedia article.
  *
  * @param title               title of the page
  * @param text                plain text of the page
  * @param textlinks           all links appearing in the text
  * @param templatelinks       all links appearing in wikitext templates (e.g. infoboxes)
  * @param foundaliases        all aliases (of all links) found in the plain text
  * @param categorylinks       all links of category pages on the page
  * @param listlinks           all links appearing in lists
  * @param disambiguationlinks all links on this page if this page is a disambiguation page
  * @param linkswithcontext    all textlinks containing the term frequencies of their context
  * @param context             term frequencies of this articles plain text
  * @param triealiases         the longest aliases found by the trie with their offset and context
  * @param rawextendedlinks    all occurrences of aliases of other links in this article
  * @param textlinksreduced    all links appearing in the text with aliases of companies
  * @param templatelinksreduced all links appearing in wikitext templates (e.g. infoboxes) with aliases of companies
  * @param categorylinksreduced all links of category pages on the page with aliases of companies
  * @param listlinksreduced    all links appearing in lists with aliases of companies
  * @param disambiguationlinksreduced all links on this page if this page is a disambiguation page with aliases of
  *                                   companies
  */
case class ParsedWikipediaEntry(
    title: String,
    var text: Option[String] = None,
    var textlinks: List[Link] = Nil,
    var templatelinks: List[Link] = Nil,
    var foundaliases: List[String] = Nil,
    var categorylinks: List[Link] = Nil,
    var listlinks: List[Link] = Nil,
    var disambiguationlinks: List[Link] = Nil,
    var linkswithcontext: List[Link] = Nil,
    var context: Map[String, Int] = Map[String, Int](),
    var triealiases: List[TrieAlias] = Nil,
    var rawextendedlinks: List[ExtendedLink] = Nil,
    var textlinksreduced: List[Link] = Nil,
    var templatelinksreduced: List[Link] = Nil,
    var categorylinksreduced: List[Link] = Nil,
    var listlinksreduced: List[Link] = Nil,
    var disambiguationlinksreduced: List[Link] = Nil
) {
    def setText(t: String): Unit = text = Option(t)

    def getText(): String = text.getOrElse("")

    /**
      * Concatenates all link lists extracted from the Wikipedia article.
      *
      * @return all links
      */
    def allLinks(): List[Link] = {
        List(
            textlinks,
            templatelinks,
            categorylinks,
            listlinks,
            disambiguationlinks,
            extendedlinks()).flatten
    }

    /**
      * Concatenates all reduced link lists extracted from the Wikipedia article.
      * @return all reduced links
      */
    def reducedLinks(): List[Link] = {
        List(
            textlinksreduced,
            templatelinksreduced,
            categorylinksreduced,
            listlinksreduced,
            disambiguationlinksreduced,
            extendedlinks()).flatten
    }

    /**
      * Executes a filter function on every not reduced link list.
      *
      * @param filterFunction filter function
      */
    def filterLinks(filterFunction: Link => Boolean): Unit = {
        textlinks = textlinks.filter(filterFunction)
        templatelinks = templatelinks.filter(filterFunction)
        categorylinks = categorylinks.filter(filterFunction)
        disambiguationlinks = disambiguationlinks.filter(filterFunction)
        listlinks = listlinks.filter(filterFunction)
    }

    /**
      * Executes a filter function on every not reduced link list and saves the result to the reduced columns.
      * @param filterFunction filter function
      */
    def reduceLinks(filterFunction: Link => Boolean): Unit = {
        textlinksreduced = textlinks.filter(filterFunction)
        templatelinksreduced = templatelinks.filter(filterFunction)
        categorylinksreduced = categorylinks.filter(filterFunction)
        disambiguationlinksreduced = disambiguationlinks.filter(filterFunction)
        listlinksreduced = listlinks.filter(filterFunction)
    }

    /**
      * Filters the linkswithcontext links and returns only the textlinks with their contexts.
      *
      * @return textlinks and their contexts
      */
    def textlinkContexts(): List[Link] = {
        linkswithcontext.filter(contextLink =>
            textlinks.exists(link => link.alias == contextLink.alias && link.offset == contextLink.offset))
    }

    /**
      * Filters the linkswithcontext links and returns only the extendedlinks with their contexts.
      *
      * @return extendedlinks and their contexts
      */
    def extendedlinkContexts(): List[Link] = {
        linkswithcontext.filter(contextLink =>
            extendedlinks().exists(link => link.alias == contextLink.alias && link.offset == contextLink.offset))
    }

    /**
      * Filters and parses Extended Links to Links
      *
      * @param noTextLinks defines if you do not want extended links that are colliding with text links, default is true
      * @param countThresh defines the threshold of occurrences of alias it has to pass
      *                    if there are colliding extended links
      * @param normalizedThresh defines the threshold of normalized occurrences of alias it has to pass
      *                         if there are colliding extended links
      * @return List of (filtered) Links
      */
    def extendedlinks(noTextLinks: Boolean = true, countThresh: Int = 1, normalizedThresh: Double = 0.1): List[Link] = {
        var filteredLinks = rawextendedlinks
            .map(t => (t, t.filterExtendedLink(countThresh, normalizedThresh)))
            .collect {
                case (t, Some(page)) => (t, page)
            }.map { case (exLink, page) =>
            Link(exLink.alias, page, exLink.offset)
        }
        if(noTextLinks) {
            filteredLinks = filterCollidingLinks(filteredLinks, textlinks)
        }
        filteredLinks
    }

    /**
      * Filters extended links by overlapping offsets of text links in O(nlogn).
      *
      * @param extendedLinks List of extended links that will be filtered
      * @param textLinks     List of text links that is used to filter
      * @return filtered List of extended links without colliding text links
      */
    def filterCollidingLinks(extendedLinks: List[Link], textLinks: List[Link]): List[Link] = {
        val orderedExtendedLinks = extendedLinks.filter(_.offset.exists(_ >= 0)).sortBy(_.offset)
        val orderedTextLinks = textLinks.filter(_.offset.exists(_ >= 0)).sortBy(_.offset)
        var resultList = new ListBuffer[Link]()
        var (i, j) = (0, 0)
        while(j < orderedTextLinks.length && i < orderedExtendedLinks.length) {
            val startEL = orderedExtendedLinks(i).offset.get
            val endEL = startEL + orderedExtendedLinks(i).alias.length - 1
            var startTL = orderedTextLinks(j).offset.get
            var endTL = startTL + orderedTextLinks(j).alias.length - 1
            // find the next text link that might be colliding
            while(endTL < startEL && j < orderedTextLinks.length - 1) {
                j += 1
                startTL = orderedTextLinks(j).offset.get
                endTL = startTL + orderedTextLinks(j).alias.length - 1
            }
            if(!checkIfColliding(startEL, endEL, startTL, endTL)) {
                resultList += orderedExtendedLinks(i)
            }
            i += 1
        }
        // if there are no text links but extended links left
        // add the remaining extended links because they cannot collide with anything
        while(i < orderedExtendedLinks.length) {
            resultList += orderedExtendedLinks(i)
            i += 1
        }
        resultList.toList
    }

    /**
      * Checks if two links are colliding by checking their start and end offsets.
      *
      * @param startEL start offset of extended link
      * @param endEL   end offset of extended link
      * @param startTL start offset of text link
      * @param endTL   end offset of text link
      * @return true if they are colliding
      */
    def checkIfColliding(startEL: Int, endEL: Int, startTL: Int, endTL: Int): Boolean = {
        // startTL---startEL----endTL or
        (startTL <= startEL && startEL <= endTL) ||
            // startTL---endEL----endTL or
            (startTL <= endEL && endEL <= endTL) ||
            // startEL---startTL---endTL---EndEL
            (startEL <= startTL && endTL <= endEL)
    }
}
