package de.hpi.ingestion.framework

import scala.collection.mutable
import scala.io.Source
import scala.xml.{Node, XML}

/**
  * Configurable trait implements a parsing method for configuring the settings.
  */
trait Configurable {
	var configFile: String = ???
	var config: Map[String, Map[String, Any]] = Map[String, Map[String, Any]]()

	/**
	  * Reads the configuration from an xml file.
	  * @return a list containing settings and some options
	  */
	def parseConfig(
		path: String,
		options: Map[String, Node => Map[String, Any]] = Map[String, Node => Map[String, Any]]()
	): Map[String, Map[String, Any]] = {
		val xml = XML.loadString(Source
			.fromURL(getClass.getResource(s"/configs/$path"))
			.getLines()
			.mkString("\n"))

		val config = mutable.Map[String, Map[String, Any]]()
		val head = (xml \\ "config").head
		options.mapValues(_(head)) + ("settings" -> this.parseSettings(head))
	}

	private def parseSettings(head: Node): Map[String, String] = {
		val settings = head \ "settings"
		settings.head.child.collect {
			case node: Node if node.text.trim.nonEmpty => (node.label, node.text)
		}.toMap
	}
}
