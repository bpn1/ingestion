package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

/**
  * Calculates term frequency per article and link.
  */
object TermFrequencyCounter extends SparkJob {
	appName = "Term Frequency Counter"
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputArticlesTablename = "parsedwikipedia"
	val contextSize = 20

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		List(articles).toAnyRDD()
	}

	/**
	  * Saves enriched Parsed Wikipedia entries to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.saveToCassandra(keyspace, outputArticlesTablename)
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
		extractBagOfWords(tokens)
	}

	/**
	  * Extracts the stemmed terms of a list of tokens and their relative frequency (between 0.0 and 1.0).
	  *
	  * @param tokens list of tokens to be counted
	  * @return a Bag containing every term and its frequency
	  */
	def extractBagOfWords(tokens: List[String]): Bag[String, Int] = {
		Bag[String, Int](tokens)
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
	  * Extracts context of the links of a given Wikipedia entry. The contexts are {@contextSize} tokens in front of
	  * and behind the location of the links alias in the tokenized text.
	  *
	  * @param entry Wikipedia entry containing the used links
	  * @return list of tuples containing the link and its context
	  */
	def extractLinkContexts(entry: ParsedWikipediaEntry, tokenizer: IngestionTokenizer): ParsedWikipediaEntry = {
		val tokens = tokenizer.onlyTokenize(entry.getText())
		val contextLinks = entry.textlinks.map { link =>
			val aliasTokens = tokenizer.onlyTokenize(link.alias)
			val aliasIndex = tokens.indexOfSlice(aliasTokens)
			val contextStart = Math.max(aliasIndex - contextSize, 0)
			val preAliasContext = tokens.slice(contextStart, aliasIndex)
			val contextEnd = Math.min(aliasIndex + contextSize, tokens.length)
			val postAliasContext = tokens.slice(aliasIndex + aliasTokens.length, contextEnd)
			val context = preAliasContext ++ postAliasContext
			val bag = extractBagOfWords(tokenizer.process(context))
			link.copy(context = bag.getCounts())
		}
		entry.copy(linkswithcontext = contextLinks)
	}

	/**
	  * Enriches articles with the term frequencies of the whole article (called context) and the term frequencies
	  * for all link contexts (called link contexts).
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val tokenizer = IngestionTokenizer(args)
		val enrichedArticles = articles
			.map(extractArticleContext(_, tokenizer))
			.map(extractLinkContexts(_, tokenizer))
		List(enrichedArticles).toAnyRDD()
	}
}
