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
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Enriches each `ParsedWikipediaEntry` with the term frequencies of the whole article (called context) and the term
  * frequencies for all link contexts (called link contexts).
  */
object TermFrequencyCounter extends SparkJob {
	appName = "Term Frequency Counter"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(articles).toAnyRDD()
	}

	/**
	  * Saves enriched Parsed Wikipedia entries to the Cassandra.
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
	  * Extracts the stemmed terms of a Wikipedia article and their relative frequency (between 0.0 and 1.0).
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return a Bag containing every term and its frequency
	  */
	def extractBagOfWords(entry: ParsedWikipediaEntry, tokenizer: IngestionTokenizer): Bag[String, Int] = {
		val tokens = tokenizer.process(entry.getText())
		Bag.extract(tokens)
	}

	/**
	  * Enriches a Wikipedia article with its bag of words (implemented as Map).
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return parsed Wikipedia entry with additional context information
	  */
	def extractArticleContext(entry: ParsedWikipediaEntry, tokenizer: IngestionTokenizer): ParsedWikipediaEntry = {
		val context = extractBagOfWords(entry, tokenizer).getCounts()
		entry.copy(context = context)
	}

	/**
	  * Extracts context of the links of a given Wikipedia entry. The contexts are n tokens in front of
	  * and behind the location of the links alias in the tokenized text. Extracts the links for the textlinks and the
	  * extendedlinks column
	  *
	  * @param entry Wikipedia entry containing the used links
	  * @return list of tuples containing the link and its context
	  */
	def extractLinkContexts(entry: ParsedWikipediaEntry, tokenizer: IngestionTokenizer): ParsedWikipediaEntry = {
		val tokens = tokenizer.onlyTokenizeWithOffset(entry.getText())
		val contextLinks = (entry.textlinksreduced ++ entry.extendedlinks()).map { link =>
			val context = extractContext(tokens, link, tokenizer).getCounts()
			link.copy(context = context)
		}
		entry.copy(linkswithcontext = contextLinks)
	}

	/**
	  * Extracts the context of a given link out of its article.
	  * @param tokens Offset Tokens of the links article
	  * @param link Link whose context will be extracted
	  * @param tokenizer tokenizer used to tokenize the Links alias
	  * @return Bag of words of the links context
	  */
	def extractContext(tokens: List[OffsetToken], link: Link, tokenizer: IngestionTokenizer): Bag[String, Int] = {
		val contextSize = settings("contextSize").toInt
		val aliasTokens = tokenizer.onlyTokenizeWithOffset(link.alias)
		tokens.find(token => link.offset.contains(token.beginOffset)).map { firstAliasToken =>
			val aliasIndex = tokens.indexOf(firstAliasToken)
			val aliasTokenLength = aliasTokens.length
			val contextStart = Math.max(aliasIndex - contextSize, 0)
			val preAliasContext = tokens.slice(contextStart, aliasIndex)
			val contextEnd = Math.min(aliasIndex + aliasTokenLength + contextSize, tokens.length)
			val postAliasContext = tokens.slice(aliasIndex + aliasTokens.length, contextEnd)
			val context = (preAliasContext ++ postAliasContext).map(_.token)
			Bag.extract(tokenizer.process(context))
		}.getOrElse(Bag.extract(Nil))
	}

	/**
	  * Enriches articles with the term frequencies of the whole article (called context) and the term frequencies
	  * for all link contexts (called link contexts).
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val tokenizer = IngestionTokenizer(args)
		val enrichedArticles = articles
			.map(extractArticleContext(_, tokenizer))
			.map(extractLinkContexts(_, tokenizer))
		List(enrichedArticles).toAnyRDD()
	}
}
