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

/**
  * Enriches each `ParsedWikipediaEntry` with the term frequencies of the whole article (called context) and the term
  * frequencies for all link contexts (called link contexts).
  */
class TermFrequencyCounter extends SparkJob {
	import TermFrequencyCounter._
	appName = "Term Frequency Counter"
	configFile = "textmining.xml"

	var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
	var parsedWikipediaWithTF: RDD[ParsedWikipediaEntry] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
	}

	/**
	  * Saves enriched Parsed Wikipedia entries to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * */
	override def save(sc: SparkContext): Unit = {
		parsedWikipediaWithTF.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Enriches articles with the term frequencies of the whole article (called context) and the term frequencies
	  * for all link contexts (called link contexts).
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val tokenizer = IngestionTokenizer(conf.tokenizerOpt.toList.flatten)
		parsedWikipediaWithTF = parsedWikipedia
			.map(extractArticleContext(_, tokenizer))
			.map(extractLinkContexts(_, tokenizer, settings("contextSize").toInt))
	}
}

object TermFrequencyCounter {
	/**
	  * Extracts the context of a given link out of its article.
	  * @param tokens Offset Tokens of the links article
	  * @param link Link whose context will be extracted
	  * @param tokenizer tokenizer used to tokenize the Links alias
	  * @param contextSize number of tokens before and after a match considered as the context
	  * @return Bag of words of the links context
	  */
	def extractContext(
		tokens: List[OffsetToken],
		link: Link,
		tokenizer: IngestionTokenizer,
		contextSize: Int
	): Bag[String, Int] = {
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
	  * @param tokenizer tokenizer used to generate the contexts of the entry
	  * @param contextSize number of tokens before and after a match considered as the context
	  * @return list of tuples containing the link and its context
	  */
	def extractLinkContexts(
		entry: ParsedWikipediaEntry,
		tokenizer: IngestionTokenizer,
		contextSize: Int
	): ParsedWikipediaEntry = {
		val tokens = tokenizer.onlyTokenizeWithOffset(entry.getText())
		val contextLinks = (entry.textlinksreduced ++ entry.extendedlinks()).map { link =>
			val context = extractContext(tokens, link, tokenizer, contextSize).getCounts()
			link.copy(context = context)
		}
		entry.copy(linkswithcontext = contextLinks)
	}
}
