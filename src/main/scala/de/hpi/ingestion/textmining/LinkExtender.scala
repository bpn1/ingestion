package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import de.hpi.ingestion.textmining.models._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.TextParser.{keyspace, outputTablename, tablename}


/**
  * Extend Wikipedia articles with links that were only tagged once
  */
object LinkExtender extends SparkJob {
	val keyspace = "wikidumps"
	val inputParsedTablename = "parsedwikipedia"
	val outputParsedTablename = "parsedwikipedia"
	val inputAliasToPagesTablename = "wikipedialinks"
	val inputPageToAliasesTablename = "wikipediapages"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
		List(wikipedia).toAnyRDD()
	}

	/**
	  * Saves Parsed Wikipedia entries to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.saveToCassandra(keyspace, outputTablename)
	}
	// $COVERAGE-ON$

	def findAllAliases(article: ParsedWikipediaEntry, aliasesRDD: RDD[Page]): Set[Page] = {
		val links = article.allLinks()
		val aliases = aliasesRDD.collect.toList
		val pageAliases = aliases.filter(_.page == article.title)
		val linkAliases = links.flatMap { link =>
			aliases.filter(_.page == link.page)
		}

		(pageAliases ++ linkAliases).toSet
	}

	def buildTrieFromAliases(pages: Set[Page]): TrieNode = {
		val tokenizer = IngestionTokenizer(false, false)
		val trie = new TrieNode()
		pages.foreach{ page =>
			page.aliases.foreach {case (key, value) =>
				val tokens = tokenizer.process(key)
				trie.append(tokens)
			}
		}
		trie
	}

	def findAliasOccurences(
		entry: ParsedWikipediaEntry,
		pages: Set[Page],
		trie: TrieNode,
		tokenizer: IngestionTokenizer): ParsedWikipediaEntry = {
		val resultList = ListBuffer[Link]()
		val tokens = tokenizer.process(entry.getText())
		var text = entry.getText()
		var i = 0
		var offset = 0
		while(i < tokens.length) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens)
			if (aliasMatches.nonEmpty) {
				val page = pages.filter(_.aliases isDefinedAt aliasMatches.maxBy(_.length).mkString(" ")).head
				resultList += Link(
					aliasMatches.maxBy(_.length).mkString(" "),
					page.page,
					Option(offset)
				)
				for (j <- i until i + aliasMatches.maxBy(_.length).length) {
					offset = offset + (text indexOf tokens(j)) + tokens(j).length
					text = text.slice(offset, text.length)
				}
				i += aliasMatches.maxBy(_.length).length
			}
			else {
				offset = offset + (text indexOf tokens(i)) + tokens(i).length
				text = text.slice(offset, text.length)
				i += 1
			}
		}
		entry.extendedLinks = resultList.toList
		entry
	}

	/**
	  * Groups link aliases by page names and vice versa. Also removes dead links (no corresponding page).
	  *
	  * @param articles all Wikipedia articles
	  * @return Grouped aliases and page names
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val rawArticles = input.fromAnyRDD[WikipediaEntry]().head
		List(rawArticles).toAnyRDD()
	}
}
