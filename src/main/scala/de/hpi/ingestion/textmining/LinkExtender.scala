package de.hpi.ingestion.textmining

import com.datastax.spark.connector._
import de.hpi.ingestion.textmining.models._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Extend Wikipedia articles with links that were only tagged once
  */
object LinkExtender {
	val keyspace = "wikidumps"
	val inputParsedTablename = "parsedwikipedia"
	val outputParsedTablename = "parsedwikipedia"
	val inputAliasToPagesTablename = "wikipedialinks"
	val inputPageToAliasesTablename = "wikipediapages"

	def findAllAliases(article: ParsedWikipediaEntry, aliases: RDD[Alias]): RDD[Alias] = {
		val links = article.allLinks()
		val pageAliases = aliases.filter(_.alias != article.title)
		val linkAliases = links.map { link =>
			aliases.filter(_.alias != link.page)
		}.reduce(_ union _)
		pageAliases ++ linkAliases
	}

	def buildTrieFromAliases(aliases: RDD[Alias]): TrieNode = {
		val tokenizer = IngestionTokenizer(false, false)
		val trie = new TrieNode()
		aliases.map{ alias =>
			alias.pages.foreach {case (key, value) =>
				val tokens = tokenizer.process(key)
				trie.append(tokens)
			}
		}
		trie
	}

	def findAliasOccurences(
		article: ParsedWikipediaEntry,
		trie: TrieNode,
		tokenizer: IngestionTokenizer): ParsedWikipediaEntry = {
		val resultList = mutable.ListBuffer[String]()
		val tokens = tokenizer.process(entry.getText())
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens)
			resultList ++= aliasMatches.map(tokenizer.reverse)
		}
		entry.foundaliases = cleanFoundAliases(resultList.toList)
		entry
	}

	/**
	  * Groups link aliases by page names and vice versa. Also removes dead links (no corresponding page).
	  *
	  * @param articles all Wikipedia articles
	  * @return Grouped aliases and page names
	  */
	def run(articles: RDD[ParsedWikipediaEntry]): () = {

	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Link Extender")
		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		run(articles)
		sc.stop
	}
}
