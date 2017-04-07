package de.hpi.ingestion.textmining.models

import org.jsoup.nodes.{Element, Node, TextNode}
import org.jsoup.select.NodeVisitor

class WikipediaNodeVisitor(validTags: List[String] = List("a")) extends NodeVisitor {

	var cleanedDocument = ""

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

	override def tail(node: Node, depth: Int): Unit = {}

	def getCleanedDocument(): String = cleanedDocument
}
