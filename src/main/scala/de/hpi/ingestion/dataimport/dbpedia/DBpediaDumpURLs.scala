package de.hpi.ingestion.dataimport.dbpedia

import org.jsoup.Jsoup
import scala.io.Source
import scala.collection.JavaConverters._

object DBpediaDumpURLs {
	val latestDumpURL = "http://wiki.dbpedia.org/Downloads"

	def main(args: Array[String]) {
		if(args.isEmpty) {
			println("Usage: DBpediaDumpURLs path/to/dbpedia.html")
			println(s"Download latest overview from $latestDumpURL")
			return
		}
		val html = Source.fromFile(args(0)).getLines.mkString("\n")
		val doc = Jsoup.parse(html)
		doc
			.select("a:contains(ttl)")
			.asScala
			.map(_.attr("href"))
			.filter(!_.contains("en_uris"))
			.foreach(println)
	}
}
