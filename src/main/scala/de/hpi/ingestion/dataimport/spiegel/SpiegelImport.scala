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

package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import play.api.libs.json.JsValue
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.textmining.models.TrieAliasArticle

/**
  * Parses the Spiegel JSON dump to Articles, parses the HTML to raw text and saves them to the Cassandra.
  */
class SpiegelImport extends SparkJob with JSONParser[TrieAliasArticle] {
    import SpiegelImport._
    appName = "Spiegel Import"
    configFile = "textmining.xml"

    var spiegelDump: RDD[String] = _
    var parsedArticles: RDD[TrieAliasArticle] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Spiegel JSON dump from the HDFS.
      * @param sc   Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        spiegelDump = sc.textFile(settings("spiegelFile"))
    }

    /**
      * Saves the parsed Spiegel Articles to the cassandra.
      * @param sc     Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        parsedArticles.saveToCassandra(settings("keyspace"), settings("spiegelTable"))
    }
    // $COVERAGE-ON$

    /**
      * Parses the Spiegel JSON dump into Spiegel Articles.
      * @param sc    Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        parsedArticles = spiegelDump.map(parseJSON)
    }

    /**
      * Extracts the article data from a given JSON object, parses the HTML content into text and finds the occurring
      * aliases in the text.
      *
      * @param json JSON object containing the article data
      * @return Spiegel Article containing the parsed JSON data
      */
    override def fillEntityValues(json: JsValue): TrieAliasArticle = {
        val id = extractString(json, List("_id", "$oid")).get
        val title = extractString(json, List("title"))
            .map(_.replaceAll("&nbsp;", " "))
        val content = extractString(json, List("content"))
        val text = content
            .map(_.replaceAll("&nbsp;", " "))
            .map(extractArticleText)
        TrieAliasArticle(id, title, text)
    }
}

object SpiegelImport {
    /**
      * Extracts the text contents of the HTML page.
      *
      * @param html HTML page as String
      * @return text contents of the page
      */
    def extractArticleText(html: String): String = {
        val contentTags = List("div.spArticleContent", "div.dig-artikel", "div.article-section")
        val doc = Jsoup.parse(html)
        val title = doc.head().text()
        val content = contentTags
            .map(doc.select)
            .find(_.size == 1)
            .map(_.text())
            .getOrElse(doc.body.text())
        s"$title $content".trim
    }
}
