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

import org.jsoup.nodes.{Element, Node, TextNode}
import org.jsoup.select.NodeVisitor

/**
  * This class implements the Jsoup NodeVisitor Interface and is used to traverse the HTML tree and remove any
  * unwanted Tags.
  *
  * @param validTags list of tags as Strings which should be kept in the document. The default tag to be kept is "a".
  */
class WikipediaNodeVisitor(validTags: List[String] = List("a")) extends NodeVisitor {

    /**
      * Stores the built HTML document String.
      */
    var cleanedDocument = ""

    /**
      * This method is called when a node is first visited. It filters any HTML-tag which is not an anchor and also
      * filters the text nodes which are children of anchor tags. If a node is not filtered, its HTML is appended to
      * cleanedDocument .
      *
      * @param node  the node which is visited
      * @param depth the depth of the node
      */
    override def head(node: Node, depth: Int): Unit = {
        node match {
            case t: TextNode =>
                if(!validTags.contains(t.parent().asInstanceOf[Element].tag().toString)) {
                    cleanedDocument += t.text()
                }
            case t: Element =>
                if(validTags.contains(t.tag.toString)) {
                    cleanedDocument += t.outerHtml()
                }
        }
    }

    /**
      * This method is called when a node is exited. It doesn't do anything.
      *
      * @param node  the node which is visited
      * @param depth the depth of the node
      */
    override def tail(node: Node, depth: Int): Unit = {}

    /**
      * Returns the built HTML document as String.
      *
      * @return the built HTML document as String
      */
    def getCleanedDocument(): String = cleanedDocument
}
