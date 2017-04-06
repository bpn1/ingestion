package de.hpi.ingestion.textmining.models

import org.jsoup.nodes.Node
import org.jsoup.select.NodeVisitor
import scala.collection.JavaConversions._

case class AnchorNodeVisitor(var childList: List[String]) extends NodeVisitor {
	// http://stackoverflow.com/questions/37384125/how-to-traverse-recursively-an-html-tree-with-jsoup
	if (childList == null) throw new NullPointerException("child cannot be null.")

	override def head(node: Node, depth: Int): Unit = {
		if (node.childNodeSize == 0) {
			childList.add(node.toString)
		}
	}

	override def tail(node: Node, depth: Int): Unit = {

	}
}
