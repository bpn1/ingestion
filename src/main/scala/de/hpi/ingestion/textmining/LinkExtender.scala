package de.hpi.ingestion.textmining

import org.apache.spark.SparkContext
import com.datastax.spark.connector._

import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import de.hpi.ingestion.framework.SparkJob


/**
  * Extends Wikipedia articles with links that were only tagged once
  */
object LinkExtender extends SparkJob {
	val keyspace = "wikidumps"
	val inputParsedTablename = "parsedwikipedia"
	val outputParsedTablename = "parsedwikipedia"
	val inputPageToAliasesTablename = "wikipediapages"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		val pages = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputPageToAliasesTablename)
		List(parsedWikipedia).toAnyRDD() ++ List(pages).toAnyRDD()
	}

	/**
	  * Saves Parsed Wikipedia entries to the Cassandra.
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

	def findAllAliases(article: ParsedWikipediaEntry,
		pages: Map[String, Map[String, Int]]
	): Map[String, Map[String, Int]] = {
		val links = article.allLinks()
		val pageAliases = pages.filterKeys(_ == article.title)
		val linkAliases = links.flatMap(link => pages.filterKeys(_ == link.page))
		pageAliases ++ linkAliases
	}

	def buildTrieFromAliases(aliases: Map[String, String], tokenizer: IngestionTokenizer): TrieNode = {
		val trie = new TrieNode()
		aliases.keys.foreach { key =>
			val tokens = tokenizer.process(key)
			trie.append(tokens)
		}
		trie
	}

	def findAliasOccurences(
		entry: ParsedWikipediaEntry,
		aliases: Map[String, String],
		trie: TrieNode,
		tokenizer: IngestionTokenizer
	): ParsedWikipediaEntry = {
		val resultList = ListBuffer[Link]()
		val tokens = tokenizer.process(entry.getText())
		val text = entry.getText()
		var i = 0
		var offset = 0
		while(i < tokens.length) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens)
			if(aliasMatches.nonEmpty) {
				val found = tokenizer.reverse(aliasMatches.maxBy(_.length))
				val page = aliases(found)
				offset = text.indexOf(found, offset)
				resultList += Link(found, page, Option(offset))
				i += aliasMatches.maxBy(_.length).length
			}
			else {
				offset += tokens(i).length
				i += 1
			}
		}
		entry.extendedLinks = resultList.toList
		entry
	}

	def reversePages(pages: Map[String, Map[String, Int]]): Map[String, String] = {
		pages.toList
			.flatMap(t => t._2.map((_, t._1)))
			.groupBy(_._1._1)
			.map { alias =>
					val page = alias._2.reduce { (currentMax, currentAlias) =>
						val aliasCount = pages(currentAlias._2).values.sum
						val maxCount = pages(currentMax._2).values.sum
						if((currentAlias._1._2.toFloat / aliasCount) > (currentMax._1._2.toFloat / maxCount)) {
							currentAlias
						}
						else {
							currentMax
						}
					}._2
				(alias._1, page)
			}
	}

	/**
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
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
				val tempPages = findAllAliases(entry, localPages)
				val aliases = reversePages(tempPages)
				val trie = buildTrieFromAliases(aliases, tokenizer)
				findAliasOccurences(entry, aliases, trie, tokenizer)
			}
		}, true)

		List(parsedArticlesWithExtendedLinks).toAnyRDD()
	}
}
