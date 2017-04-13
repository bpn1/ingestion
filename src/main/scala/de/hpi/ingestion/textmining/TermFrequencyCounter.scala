package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object TermFrequencyCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputArticlesTablename = "parsedwikipedia"
	val tokenizer = new CleanCoreNLPTokenizer
	val contextSize = 20

	/**
	  * Extracts the stemmed terms of a Wikipedia article and their relative frequency (between 0.0 and 1.0).
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return a Bag containing every term and its frequency
	  */
	def extractBagOfWords(entry: ParsedWikipediaEntry): Bag[String, Int] = {
		val tokens = tokenizer.tokenize(entry.getText())
		extractBagOfWords(tokens)
	}

	/**
	  * Extracts the stemmed terms of a list of tokens and their relative frequency (between 0.0 and 1.0).
	  *
	  * @param tokens list of tokens to be counted
	  * @return a Bag containing every term and its frequency
	  */
	def extractBagOfWords(tokens: List[String]): Bag[String, Int] = {
		val stemmer = new AccessibleGermanStemmer
		val stemmedWords = tokens.map(stemmer.stem)
		Bag[String, Int](stemmedWords)
	}

	/**
	  * Enriches a Wikipedia article with its bag of words (implemented as Map)
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return parsed Wikipedia entry with additional context information
	  */
	def extractArticleContext(entry: ParsedWikipediaEntry): ParsedWikipediaEntry = {
		val context = extractBagOfWords(entry).getCounts()
		entry.copy(context = context)
	}

	/**
	  * Extracts context of the links of a given Wikipedia entry. The context are {@contextSize} tokens in front of
	  * and behind the location of the links alias in the tokenized text.
	  *
	  * @param entry Wikipedia entry containing the used links
	  * @return list of tuples containing the link and its context
	  */
	def extractLinkContexts(entry: ParsedWikipediaEntry): ParsedWikipediaEntry = {
		val tokens = tokenizer.tokenize(entry.getText())
		val contextLinks = entry.textlinks.map { link =>
			val aliasTokens = tokenizer.tokenize(link.alias)
			val aliasIndex = tokens.indexOfSlice(aliasTokens)
			val contextStart = Math.max(aliasIndex - contextSize, 0)
			val preAliasContext = tokens.slice(contextStart, aliasIndex)
			val contextEnd = Math.min(aliasIndex + contextSize, tokens.length)
			val postAliasContext = tokens.slice(aliasIndex + aliasTokens.length, contextEnd)
			val context = preAliasContext ++ postAliasContext
			val bag = extractBagOfWords(context)
			link.copy(context = bag.getCounts())
		}
		entry.copy(linkswithcontext = contextLinks)
	}

	/**
	  * Enriches links from all articles with their contexts (bag of words, implemented as Map)
	  *
	  * @param articles parsed Wikipedia articles which links will be enriched with their contexts
	  * @return links with additional context information
	  */
	def addLinksContexts(articles: RDD[ParsedWikipediaEntry]): RDD[ParsedWikipediaEntry] = {
		articles.map(extractLinkContexts)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Term Frequency Counter")

		val sc = new SparkContext(conf)
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)

		val articlesWithContext = articles
			.map(extractArticleContext)

		addLinksContexts(articlesWithContext)
			.saveToCassandra(keyspace, outputArticlesTablename)

		sc.stop()
	}
}
