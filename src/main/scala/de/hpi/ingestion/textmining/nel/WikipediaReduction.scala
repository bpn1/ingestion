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

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{ParsedWikipediaEntry, TrieAliasArticle}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Reduces each `ParsedWikipediaEntry` to its relevant attributes for NEL.
  */
class WikipediaReduction extends SparkJob {
    appName = "Wikipedia Reduction"
    configFile = "textmining.xml"

    var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
    var reducedArticles: RDD[TrieAliasArticle] = _

    // $COVERAGE-OFF$
    /**
      * Loads Wikipedia entries and aliases from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
    }

    /**
      * Save reduced Wikipedia articles to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        reducedArticles.saveToCassandra(settings("keyspace"), settings("wikipediaNELTable"))
    }
    // $COVERAGE-ON$

    /**
      * Reduces each `ParsedWikipediaEntry` to its relevant attributes for NEL.
      * @param sc Spark Context
      */
    override def run(sc: SparkContext): Unit = {
        reducedArticles = parsedWikipedia.map(article => TrieAliasArticle(
            id = article.title,
            title = Option(article.title),
            text = article.text
        ))
    }
}
