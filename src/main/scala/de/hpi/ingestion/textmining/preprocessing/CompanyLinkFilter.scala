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
import de.hpi.ingestion.dataimport.wikidata.models.WikidataEntity
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{Page, ParsedWikipediaEntry}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Removes all Wikipedia `Links` with an alias that never points to a company.
  */
class CompanyLinkFilter extends SparkJob {
	import CompanyLinkFilter._
	appName = "Company Link Filter"
	configFile = "textmining.xml"

	var wikidataEntities: RDD[WikidataEntity] = _
	var pages: RDD[Page] = _
	var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
	var cleanedParsedWikipedia: RDD[ParsedWikipediaEntry] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Wikidata entries, Parsed Wikipedia entries and Wikipedia pages from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		wikidataEntities = sc.cassandraTable[WikidataEntity](settings("keyspace"), settings("wikidataTable"))
		pages = sc.cassandraTable[Page](settings("keyspace"), settings("pageTable"))
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
	  * Removes all non company links from each ParsedWikipediaEntry.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val companyPages = extractCompanyPages(wikidataEntities)
		val companyAliases = extractCompanyAliases(pages, companyPages).collect.toSet
		val aliasBroadcast = sc.broadcast(companyAliases)

		cleanedParsedWikipedia = parsedWikipedia.mapPartitions({ partition =>
			val localCompAliases = aliasBroadcast.value
			partition.map(filterCompanyLinks(_, localCompAliases))
		}, true)
	}
}

object CompanyLinkFilter {
	/**
	  * Extracts Wikipedia page names of every tagged Wikidata entity.
	  * @param wikidata RDD of Wikidata entities
	  * @return RDD of Wikipedia article names
	  */
	def extractCompanyPages(wikidata: RDD[WikidataEntity]): RDD[String] = {
		wikidata
			.filter(entry => entry.instancetype.isDefined && entry.wikiname.isDefined)
			.map(_.wikiname.get)
	}

	/**
	  * Extracts all aliases companies have.
	  * @param pages RDD of Wikipedia Pages containing all aliases of every page
	  * @param companyPages RDD containing the names of all company articles
	  * @return RDD containing all aliases companies have
	  */
	def extractCompanyAliases(pages: RDD[Page], companyPages: RDD[String]): RDD[String] = {
		val joinableComps = companyPages.map(t => Tuple2(t, t))
		pages
			.map(page => (page.page, page.aliases.keySet))
			.join(joinableComps)
			.flatMap { case (page, (aliases, page2)) => aliases }
			.distinct
	}

	/**
	  * Removes all Links of a Parsed Wikipedia entry whose alias is not in the List of company aliases.
	  * @param article Parsed Wikipedia entry to be cleaned
	  * @param companyAliases List of all aliases companies have
	  * @return Parsed Wikipedia entry with cleaned links
	  */
	def filterCompanyLinks(article: ParsedWikipediaEntry, companyAliases: Set[String]): ParsedWikipediaEntry = {
		article.reduceLinks(link => companyAliases.contains(link.alias))
		article
	}
}
