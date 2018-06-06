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
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Groups `Aliases` of `Links` by page names and vice versa.
  */
class LinkAnalysis extends SparkJob {
    import LinkAnalysis._
    appName = "Link Analysis"
    configFile = "textmining.xml"
    cassandraSaveQueries += s"TRUNCATE TABLE ${settings("keyspace")}.${settings("linkTable")}"

    var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
    var pages: RDD[Page] = _
    var aliases: RDD[Alias] = _

    // $COVERAGE-OFF$
    /**
      * Loads Parsed Wikipedia entries from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
    }

    /**
      * Saves the grouped aliases and links to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        aliases
            .map(alias => (alias.alias, alias.pages))
            .saveToCassandra(settings("keyspace"), settings("linkTable"), SomeColumns("alias", "pages"))
        pages
            .map(page => (page.page, page.aliases))
            .saveToCassandra(settings("keyspace"), settings("pageTable"), SomeColumns("page", "aliases"))
    }
    // $COVERAGE-ON$

    /**
      * Groups the links once on the aliases and once on the pages.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val validLinks = extractValidLinks(parsedWikipedia, conf.toReduced)
        aliases = groupByAliases(validLinks)
        pages = groupByPageNames(validLinks)
    }
}

object LinkAnalysis {
    /**
      * Extracts links from Wikipedia articles that point to an existing page.
      * @param articles all Wikipedia articles
      * @param toReduced whether or not the reduced link columns are used
      * @return valid links
      */
    def extractValidLinks(articles: RDD[ParsedWikipediaEntry], toReduced: Boolean = false): RDD[Link] = {
        val joinableExistingPages = articles.map(article => (article.title, article.title))
        articles
            .flatMap { entry =>
                val links = if(toReduced) entry.reducedLinks() else entry.allLinks()
                links.map(l => (l.page, l))
            }.join(joinableExistingPages)
            .values
            .map { case (link, page) => link }
    }

    /**
      * Creates RDD of Aliases with corresponding pages.
      *
      * @param links links of Wikipedia articles
      * @return RDD of Aliases
      */
    def groupByAliases(links: RDD[Link]): RDD[Alias] = {
        val aliasData = links.map(link => (link.alias, List(link.page)))
        groupLinks(aliasData).map { case (alias, pages) => Alias(alias, pages) }
    }

    /**
      * Creates RDD of pages with corresponding Aliases.
      *
      * @param links links of Wikipedia articles
      * @return RDD of pages
      */
    def groupByPageNames(links: RDD[Link]): RDD[Page] = {
        val pageData = links.map(link => (link.page, List(link.alias)))
        groupLinks(pageData).map { case (page, aliases) => Page(page, aliases) }
    }

    /**
      * Groups link data on the key, counts the number of elements per key and filters entries with an empty field.
      * @param links RDD containing the link data in a predefined format
      * @return RDD containing the counted and filtered link data
      */
    def groupLinks(links: RDD[(String, List[String])]): RDD[(String, Map[String, Int])] = {
        links
            .reduceByKey(_ ++ _)
            .map { case (key, valueList) =>
                val countedValues = valueList
                    .filter(_.nonEmpty)
                    .countElements()
                (key, countedValues)
            }.filter(t => t._1.nonEmpty && t._2.nonEmpty)
    }
}
