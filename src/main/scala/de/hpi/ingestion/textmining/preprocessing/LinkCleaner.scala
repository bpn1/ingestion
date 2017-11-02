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

package de.hpi.ingestion.textmining.preprocessing

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Removes dead links that do not point to an existing page.
  */
class LinkCleaner extends SparkJob {
    import LinkCleaner._
    appName = "Link Cleaner"
    configFile = "textmining.xml"

    var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
    var cleanedParsedWikipedia: RDD[ParsedWikipediaEntry] = _

    // $COVERAGE-OFF$
    /**
      * Loads Parsed Wikipedia entries from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
    }

    /**
      * Saves cleaned Parsed Wikipedia entries to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        cleanedParsedWikipedia.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
    }
    // $COVERAGE-ON$

    /**
      * Removes dead links that do not point to an existing page from the Parsed Wikipedia entries.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val existingPages = parsedWikipedia.map(_.title)
        val validPages = groupValidPages(parsedWikipedia, existingPages)
        cleanedParsedWikipedia = filterArticleLinks(parsedWikipedia, validPages)
    }
}

object LinkCleaner {
    /**
      * Groups existing page names by the article names in which their links occur.
      *
      * @param articles      all Wikipedia articles
      * @param existingPages names of all existing Wikipedia articles
      * @return Wikipedia page names and their corresponding valid referenced page names
      */
    def groupValidPages(
        articles: RDD[ParsedWikipediaEntry],
        existingPages: RDD[String]
    ): RDD[(String, Set[String])] = {
        articles
            .flatMap(article => article.allLinks().map(link => (link.page, article.title)))
            .join(existingPages.map(page => Tuple2(page, page)))
            .values
            .map { case (title, page) => (title, Set(page)) }
            .reduceByKey(_ ++ _)
    }

    /**
      * Filters links of Wikipedia articles and discards links that do not point to a valid page.
      *
      * @param articles   all Wikipedia articles
      * @param validPages Wikipedia page names and their corresponding valid referenced page names
      * @return Wikipedia articles without links to non-existing pages
      */
    def filterArticleLinks(
        articles: RDD[ParsedWikipediaEntry],
        validPages: RDD[(String, Set[String])]
    ): RDD[ParsedWikipediaEntry] = {
        articles
            .map(article => (article.title, article))
            .join(validPages)
            .values
            .map { case (article, links) =>
                article.filterLinks(link => links.contains(link.page))
                article
            }
    }
}
