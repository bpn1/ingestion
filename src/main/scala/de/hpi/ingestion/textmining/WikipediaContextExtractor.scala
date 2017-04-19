package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{DocumentFrequency, LinkContext, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaContextExtractor {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputDocumentFrequenciesTablename = "wikipediadocfreq"
	val outputContextsTablename = "wikipediacontexts"
	val removeStopwords = true
	val leastSignificantDocumentFrequency = 5

	def countDocumentFrequencies(
		articles: RDD[ParsedWikipediaEntry]
	): RDD[DocumentFrequency] = {
		val tokenizer = new CleanCoreNLPTokenizer
		val ingestionTokenizer = new IngestionTokenizer(tokenizer, removeStopwords, true)
		articles
			.flatMap(article =>
				ingestionTokenizer.process(article.getText())
					.toSet
			)
			.map(word => (word, 1))
			.reduceByKey(_ + _)
			.map { case (word, count) => DocumentFrequency(word, count) }
	}

	def filterDocumentFrequencies(
		documentFrequencies: RDD[DocumentFrequency],
		threshold: Int
	): RDD[DocumentFrequency] = {
		documentFrequencies
			.filter(documentFrequency => documentFrequency.count >= threshold)
	}


	def extractLinkContextsFromArticle(
		article: ParsedWikipediaEntry,
		tokenizer: Tokenizer): Set[LinkContext] = {
		// Do not initialize tokenizer here so it is initialized only once.
		// (probably important for good performance of CoreNLP)
		val ingestionTokenizer = new IngestionTokenizer(tokenizer, false, false)
		val wordSet = ingestionTokenizer.process(article.getText()).toSet
		article.allLinks
			.map(link => LinkContext(link.page, wordSet))
			.toSet
	}

	def extractAllContexts(articles: RDD[ParsedWikipediaEntry]): RDD[LinkContext] = {
		val tokenizer = new CleanCoreNLPTokenizer
		articles
			.flatMap(article => extractLinkContextsFromArticle(article, tokenizer))
			.map(linkContext => (linkContext.pagename, linkContext.words))
			.reduceByKey(_ ++ _)
			.map { case (pagename, contextSum) => LinkContext(pagename, contextSum) }
	}

	def getDocumentFrequency(
		word: String,
		documentFrequencyTable: RDD[DocumentFrequency],
		frequencyThreshold: Int
	): Int = {
		val documentFrequency = documentFrequencyTable
			.filter(_.word == word)
			.collect
			.toList

		assert(documentFrequency.length <= 1) // otherwise word is not a primary key
		if (documentFrequency.nonEmpty) {
			documentFrequency.head.count
		}
		else {
			frequencyThreshold - 1
		}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Context Extractor")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val documentFrequencies = countDocumentFrequencies(allArticles)
		filterDocumentFrequencies(
			documentFrequencies,
			leastSignificantDocumentFrequency
		)
			.saveToCassandra(keyspace, outputDocumentFrequenciesTablename)
		//		extractAllContexts(allArticles)
		//			.saveToCassandra(keyspace, outputContextsTablename)

		sc.stop()
	}
}
