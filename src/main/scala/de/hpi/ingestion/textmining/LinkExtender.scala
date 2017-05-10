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
	val keyspace = "wikidumps"
	val inputParsedTablename = "parsedwikipedia"
	val outputParsedTablename = "parsedwikipedia"
	val inputPageToAliasesTablename = "wikipediapages"

	// $COVERAGE-OFF$
	/**
	  * Loads parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		val pages = sc.cassandraTable[Page](keyspace, inputPageToAliasesTablename)
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
			.saveToCassandra(keyspace, outputParsedTablename)
	}

	// $COVERAGE-ON$

	/**
	  * Filters all Pages for the Links and title of an Wikipedia article.
	  *
	  * @param article Wikipedia article to be used
	  * @param pages   raw Pages map
	  * @return filtered Pages map
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
	  * @param aliases   Aliases to be used
	  * @param tokenizer tokenizer to be uses to process Aliases
	  * @return built trie
	  */
	def buildTrieFromAliases(aliases: Map[String, String], tokenizer: IngestionTokenizer): TrieNode = {
		val trie = new TrieNode()
		aliases.keys.foreach(alias => trie.append(tokenizer.process(alias)))
		trie
	}

	/**
	  * Finds all Aliases in a given text of already linked entities and adds new Links
	  * for all occurrences.
	  *
	  * @param entry     Wikipedia entry to be extended
	  * @param aliases   map of Alias to Page
	  * @param trie      prebuilt trie from Aliases
	  * @param tokenizer tokenizer to be used to process text
	  * @return Wikipedia entry with extended Links
	  */
	def findAliasOccurrences(
		entry: ParsedWikipediaEntry,
		aliases: Map[String, String],
		trie: TrieNode,
		tokenizer: IngestionTokenizer
	): ParsedWikipediaEntry = {
		val resultList = ListBuffer[Link]()
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
				val page = aliases(found)
				val offset = longestMatch.head.beginOffset
				resultList += Link(found, page, Option(offset))
				i += longestMatch.length - 1
			}
		}
		entry.extendedlinks = resultList.toList
		entry
	}

	/**
	  * Reverts map of Page to Aliases to a map of Alias to Page.
	  *
	  * @param pages Map of Page to Aliases
	  * @return Map of Alias to Page
	  */
	def reversePages(
		pages: Map[String, Map[String, Int]],
		tokenizer: IngestionTokenizer
	): Map[String, String] = {
		pages.toList
			.flatMap(t => t._2.map(kv => (kv._1, kv._2, t._1)))
			.filter{ case (alias, count, page) =>
				alias.length != 1 && (alias.charAt(0).isLetter || alias.charAt(0).isDigit)
			}
			.groupBy(_._1)
			.map { case (alias, pageList) =>
				val page = pageList
					.reduce { (currentMax, currentAlias) =>
						val aliasCount = pages(currentAlias._3).values.sum
						val maxCount = pages(currentMax._3).values.sum
						val normalizedAliasCount = currentAlias._2.toFloat / aliasCount
						val normalizedMaxCount = currentMax._2.toFloat / maxCount
						if(normalizedAliasCount > normalizedMaxCount) currentAlias else currentMax
				}._3
				(alias, page)
			}
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
