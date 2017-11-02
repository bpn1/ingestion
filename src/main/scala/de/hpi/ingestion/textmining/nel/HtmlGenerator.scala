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

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{Article, TrieAliasArticle}
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

/**
  * Generates HTML from found links to named entities and their corresponding raw articles.
  */
class HtmlGenerator extends SparkJob {
    import HtmlGenerator._
    appName = "HTML Generator"
    configFile = "textmining.xml"

    var nelArticles: RDD[TrieAliasArticle] = _
    var htmlArticles: RDD[Article] = _

    // $COVERAGE-OFF$
    /**
      * Loads articles with their found links to named entities from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        nelArticles = sc.cassandraTable[TrieAliasArticle](settings("keyspace"), settings("NELTable"))
    }

    /**
      * Saves HTML articles containing links to named entities to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        htmlArticles.saveToCassandra(settings("keyspace"), settings("linkedArticlesTable"))
    }
    // $COVERAGE-ON$

    /**
      * Generates HTML from found links to named entities and their corresponding raw articles.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        htmlArticles = nelArticles.map(generateArticleWithLinks)
    }
}

object HtmlGenerator {
    val defaultTitle = "*** No title ***"

    /**
      * Inserts HTML links into an article text where named entities were found.
      *
      * @param article article with its found links to named entities
      * @return HTML containing links
      */
    def generateArticleWithLinks(article: TrieAliasArticle): Article = {
        val title = article.title
            .filter(_.nonEmpty)
            .getOrElse(defaultTitle)
        var html = s"<h1>$title</h1>\n"
        val text = article.getText
        var processedCharacters = 0
        article.foundentities
            .groupBy(_.offset)
            .toList
            .sortBy(_._1)
            .foreach { case (offset, groupedLinks) =>
                val distinctLinks = groupedLinks.distinct // hotfix against duplicates
            val mainLink = distinctLinks.head
                html += text.substring(processedCharacters, offset.get)
                processedCharacters = offset.get
                val pages = distinctLinks
                    .map(_.page)
                    .reduce(_ + "\n" + _)
                val page = "https://de.wikipedia.org/wiki/" + mainLink.page.replaceAll(" ", "_")
                html += s"""<a href="$page" title="$pages">"""
                val end = processedCharacters + mainLink.alias.length
                html += text.substring(processedCharacters, end)
                processedCharacters += mainLink.alias.length
                html += "</a>"
            }
        html += text.substring(processedCharacters, text.length)
        Article(title, html)
    }
}
