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
	  * Enriches a Wikipedia article with its bag of words (implemented as Map)
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return parsed Wikipedia entry with additional context information
	  */
	def addArticleContext(entry: ParsedWikipediaEntry): ParsedWikipediaEntry = {
		var enrichedArticle = entry
		enrichedArticle.context = extractBagOfWords(entry).getCounts()
		enrichedArticle
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
	  * Extracts context of the links of a given Wikipedia entry. The context are {@contextSize } tokens in front of
	  * and behind the location of the links alias in the tokenized text.
	  *
	  * @param entry Wikipedia entry containing the used links
	  * @return list of tuples containing the link and its context
	  */
	def extractLinkContexts(entry: ParsedWikipediaEntry): List[(Link, Bag[String, Int])] = {
		val tokens = tokenizer.tokenize(entry.getText())
		entry.textlinks
			.map { link =>
				val aliasTokens = tokenizer.tokenize(link.alias)
				val aliasIndexStart = tokens.indexOfSlice(aliasTokens)
				val contextStart = Math.max(aliasIndexStart - contextSize, 0)
				val contextEnd = Math.min(aliasIndexStart + contextSize, tokens.length)
				val frontContext = tokens.slice(contextStart, aliasIndexStart)
				val backContext = tokens.slice(aliasIndexStart + aliasTokens.length, contextEnd)
				val context = frontContext ++ backContext
				val bag = extractBagOfWords(context)
				(link, bag)
			}
	}

	/**
	  * Enriches links from all articles with their contexts (bag of words, implemented as Map)
	  *
	  * @param articles parsed Wikipedia articles which links will be enriched with their contexts
	  * @return links with additional context information
	  */
	def addLinksContexts(articles: RDD[ParsedWikipediaEntry]): RDD[ParsedWikipediaEntry] = {
		articles
			.map { article =>
				val links = extractLinkContexts(article)
					.map { case (link, bag) =>
						val enrichedLink = link.copy()
						enrichedLink.context = bag.getCounts()
						enrichedLink
					}
				val enrichedArticle = article.copy()
				enrichedArticle.linkswithcontext = links
				enrichedArticle
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Term Frequency Counter")

		val sc = new SparkContext(conf)
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)

		val articlesWithContext = articles
			.map(addArticleContext)

		addLinksContexts(articlesWithContext)
			.saveToCassandra(keyspace, outputArticlesTablename)

		sc.stop()
	}
}
