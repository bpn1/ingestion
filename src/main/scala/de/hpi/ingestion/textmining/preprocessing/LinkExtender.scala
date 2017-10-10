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
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Extends Wikipedia articles with `Links` of occurrences of aliases that were previously linked in the same article.
  */
class LinkExtender extends SparkJob {
	import LinkExtender._
	appName = "Link Extender"
	configFile = "textmining.xml"

	var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
	var pages: RDD[Page] = _
	var extendedParsedWikipedia: RDD[ParsedWikipediaEntry] = _

	// $COVERAGE-OFF$
	/**
	  * Loads parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		pages = sc.cassandraTable[Page](settings("keyspace"), settings("pageTable"))
	}

	/**
	  * Saves parsed Wikipedia entries with extended links to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		extendedParsedWikipedia.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Extends links of parsed Wikipedia entries by looking at existing Links and
	  * adding additional not yet linked occurrences.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val pagesMap = pages
			.map { page =>
				val aliases = page.aliasesreduced
				val totalAliasOccurrences = aliases.values.sum
				val threshold = 0.005
				(page.page, aliases.filter(_._2.toFloat / totalAliasOccurrences > threshold))
			}.filter(_._2.nonEmpty)
			.collect
			.toMap
		val pagesBroadcast = sc.broadcast(pagesMap)
		val tokenizer = IngestionTokenizer(false, false)
		extendedParsedWikipedia = parsedWikipedia.mapPartitions({ entryPartition =>
			val localPages = pagesBroadcast.value
			entryPartition.map { entry =>
				val tempPages = findAllPages(entry, localPages)
				val aliases = reversePages(tempPages, tokenizer)
				val trie = buildTrieFromAliases(aliases, tokenizer)
				findAliasOccurrences(entry, aliases, trie, tokenizer)
			}
		}, true)
	}
}

object LinkExtender {
	/**
	  * Gets all Pages for the Links and title of an Wikipedia article.
	  * Using get+map instead of filter for performance of O(n) instead of O(n*n).
	  *
	  * @param article Wikipedia article to be used
	  * @param pages   raw Map of Pages to Aliases Map(page -> Map(Alias -> Alias Count))
	  * @return filtered Map of Pages appearing in the article to aliases Map(page -> Map(Alias -> Alias Count))
	  */
	def findAllPages(
		article: ParsedWikipediaEntry,
		pages: Map[String, Map[String, Int]]
	): Map[String, Map[String, Int]] = {
		val links = article.textlinksreduced
		val filteredPages = mutable.Map[String, Map[String, Int]]()
		pages.get(article.title).foreach { aliases =>
			filteredPages(article.title) = aliases
		}
		links.foreach(link =>
			pages.get(link.page).foreach { aliases =>
				filteredPages(link.page) = aliases
			})
		filteredPages.toMap
	}

	/**
	  * Builds trie from given Aliases.
	  *
	  * @param aliases   Aliases to be used Map(alias -> (page -> alias count))
	  * @param tokenizer tokenizer to be uses to process Aliases
	  * @return built trie
	  */
	def buildTrieFromAliases(aliases: Map[String, Map[String, Int]], tokenizer: IngestionTokenizer): TrieNode = {
		val trie = new TrieNode()
		aliases.keys.foreach(alias => trie.append(tokenizer.process(alias)))
		trie
	}

	/**
	  * Finds all Aliases in a given text of already linked entities and adds new Extended Links
	  * for all occurrences.
	  *
	  * @param entry     Wikipedia entry to be extended
	  * @param aliases   map of Alias to Page to alias occurrence Map(alias -> (page -> alias count))
	  * @param trie      prebuilt trie from Aliases
	  * @param tokenizer tokenizer to be used to process text
	  * @return Wikipedia entry with extended Links
	  */
	def findAliasOccurrences(
		entry: ParsedWikipediaEntry,
		aliases: Map[String, Map[String, Int]],
		trie: TrieNode,
		tokenizer: IngestionTokenizer
	): ParsedWikipediaEntry = {
		val resultList = ListBuffer[ExtendedLink]()
		val tokens = tokenizer.processWithOffsets(entry.getText())
		val text = entry.getText()
		var i = 0
		while(i < tokens.length) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens.map(_.token))
				.filter(_.nonEmpty)
				.map(t => testTokens.take(t.length))
			i += 1
			if(aliasMatches.nonEmpty) {
				val longestMatch = aliasMatches.maxBy(_.length)
				val found = text.substring(longestMatch.head.beginOffset, longestMatch.last.endOffset)
				aliases.get(found).foreach { pages =>
					val offset = longestMatch.head.beginOffset
					resultList += ExtendedLink(found, pages, Option(offset))
					i += longestMatch.length - 1
				}
			}
		}
		entry.rawextendedlinks = resultList.toList
		entry
	}

	/**
	  * Reverts map of Page to Aliases to a map of Alias to Page.
	  *
	  * @param pages Map of Page pointing to Aliases Map(page -> Map(Alias -> Alias Count))
	  * @return Map of Alias to Page Map(alias -> page)
	  */
	def reversePages(
		pages: Map[String, Map[String, Int]],
		tokenizer: IngestionTokenizer
	): Map[String, Map[String, Int]] = {
		pages.toList
			.flatMap(t => t._2.map(kv => (kv._1, t._1, kv._2)))
			.filter { case (alias, count, page) =>
				alias.length != 1 && (alias.charAt(0).isLetter || alias.charAt(0).isDigit)
			}
			.groupBy(_._1)
			.map { case (alias, pageList) =>
				(alias, pageList.map { case (alias, page, count) =>
					(page, count)
				}.toMap)
			}
			.filter(_._2.nonEmpty)
	}
}
