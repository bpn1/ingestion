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
