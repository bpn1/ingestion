package de.hpi.ingestion.textmining

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

import scala.collection.mutable


/**
  * Extends Wikipedia articles with links of occurrences of aliases that were previously
  * linked in the same article.
  */
object LinkExtender extends SparkJob {
	appName = "Link Extender"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val pages = sc.cassandraTable[Page](settings("keyspace"), settings("pageTable"))
		List(parsedWikipedia).toAnyRDD() ++ List(pages).toAnyRDD()
	}

	/**
	  * Saves parsed Wikipedia entries with extended links to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
	}
	// $COVERAGE-ON$

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
		val links = article.textlinks
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
				if(aliases.contains(found)) {
					val pages = aliases(found)
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

	/**
	  * Extends links of parsed Wikipedia entries by looking at existing Links and
	  * adding additional not yet linked occurrences.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val parsedArticles = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val pages = input(1).asInstanceOf[RDD[Page]]
			.map(page => (page.page, page.aliases))
			.collect
			.toMap
		val pagesBroadcast = sc.broadcast(pages)
		val tokenizer = IngestionTokenizer(false, false)
		val parsedArticlesWithExtendedLinks = parsedArticles.mapPartitions({ entryPartition =>
			val localPages = pagesBroadcast.value
			entryPartition.map { entry =>
				val tempPages = findAllPages(entry, localPages)
				val aliases = reversePages(tempPages, tokenizer)
				val trie = buildTrieFromAliases(aliases, tokenizer)
				findAliasOccurrences(entry, aliases, trie, tokenizer)
			}
		}, true)
		List(parsedArticlesWithExtendedLinks).toAnyRDD()
	}
}
