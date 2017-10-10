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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Counts `Alias` occurrences and merges them into previously extracted `Aliases` with their corresponding pages.
  */
class AliasCounter extends SparkJob {
	import AliasCounter._
	appName = "Alias Counter"
	configFile = "textmining.xml"
	val maximumAliasLength = 1000

	var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
	var aliasCounts: RDD[(String, Option[Int], Option[Int])] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia and Aliases from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
	}

	/**
	  * Saves Alias occurrence counts to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		aliasCounts.saveToCassandra(
			settings("keyspace"),
			settings("linkTable"),
			SomeColumns("alias", "linkoccurrences", "totaloccurrences"))
	}
	// $COVERAGE-ON$

	/**
	  * Counts alias occurrences and merges them into previously extracted aliases with their corresponding pages.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		aliasCounts = countAliases(parsedWikipedia)
			.filter(_.alias.length <= maximumAliasLength)
			.map(entry => (entry.alias, entry.linkoccurrences, entry.totaloccurrences))
	}
}

object AliasCounter {
	/**
	  * Extracts list of link and general Alias occurrences for an article.
	  *
	  * @param entry article from which the Aliases will be extracted
	  * @return list of Aliases each with an occurrence set to 1
	  */
	def extractAliasList(entry: ParsedWikipediaEntry): List[Alias] = {
		val links = entry.allLinks().map(_.alias)
		val linkCount = links.map(Alias(_, linkoccurrences = Option(1), totaloccurrences = Option(1)))
		val aliasCount = entry.foundaliases
			.diff(linkCount.map(_.alias))
			.map(Alias(_, linkoccurrences = Option(0), totaloccurrences = Option(1)))
		linkCount ++ aliasCount
	}

	/**
	  * Reduces two AliasCounters of the same alias.
	  *
	  * @param alias1 first Alias with the same alias as alias2
	  * @param alias2 second Alias with the same alias as alias1
	  * @return Alias with summed link and total occurrences
	  */
	def aliasReduction(alias1: Alias, alias2: Alias): Alias = {
		Alias(
			alias1.alias,
			alias1.pages ++ alias2.pages,
			alias1.pagesreduced ++ alias2.pagesreduced,
			Option(alias1.linkoccurrences.get + alias2.linkoccurrences.get),
			Option(alias1.totaloccurrences.get + alias2.totaloccurrences.get)
		)
	}

	/**
	  * Counts link and total occurrences of link aliases in Wikipedia articles.
	  *
	  * @param articles RDD containing parsed wikipedia articles
	  * @return RDD containing alias counts for links and total occurrences
	  */
	def countAliases(articles: RDD[ParsedWikipediaEntry]): RDD[Alias] = {
		articles
			.flatMap(extractAliasList)
			.map(alias => (alias.alias, alias))
			.reduceByKey(aliasReduction)
			.map(_._2)
			.filter(_.alias.nonEmpty)
	}
}
