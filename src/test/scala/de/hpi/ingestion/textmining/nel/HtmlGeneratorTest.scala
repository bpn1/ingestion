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

package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.Article
import org.scalatest.{FlatSpec, Matchers}
import org.jsoup.Jsoup
import com.holdenkarau.spark.testing.SharedSparkContext
import scala.util.matching.Regex

class HtmlGeneratorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Articles with links" should "not be empty" in {
		val articles = sc.parallelize(TestData.articlesWithFoundLinks().toList)
		val input = List(articles).toAnyRDD()
		val articlesWithLinks = HtmlGenerator.run(input, sc)
			.fromAnyRDD[Article]()
			.head
		articlesWithLinks should not be empty
	}

	they should "contain html links" in {
		val anchorRegex = new Regex("(?s)<a href=\".+?\">.+?<\\/a>")
		val articles = sc.parallelize(TestData.articlesWithFoundLinks().toList)
		val input = List(articles).toAnyRDD()
		HtmlGenerator.run(input, sc)
			.fromAnyRDD[Article]()
			.head
			.collect
			.foreach { article =>
				if(article.title != "Audi Test ohne Link") {
					anchorRegex.findFirstIn(article.text) should not be empty
				}
			}
	}

	they should "only contain the title and the original text" in {
		val articles = sc.parallelize(TestData.articlesWithFoundLinks().toList)
		val input = List(articles).toAnyRDD()
		val originalTexts = articles
			.map(article => article.title.getOrElse(HtmlGenerator.defaultTitle) + "\n" + article.getText)
			.collect
			.toSet
		val texts = HtmlGenerator.run(input, sc)
			.fromAnyRDD[Article]()
			.head
			.map { article =>
				// Escape newlines with an improbable string. Otherwise jsoup will replace them with spaces.
				val escapedNewlines = Jsoup.parse(article.text.replaceAll("\n", "#EscapedNewline")).body.text
				escapedNewlines.replaceAll("#EscapedNewline", "\n")
			}.collect
			.toSet
		texts shouldEqual originalTexts
	}
}
