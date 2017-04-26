package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{DocumentFrequency, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

/**
  * Calculates document frequencies over all articles.
  */
object DocumentFrequencyCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputDocumentFrequenciesTablename = "wikipediadocfreq"
	val removeStopwords = true
	val stem = true
	val leastSignificantDocumentFrequency = 5

	/**
	  * Counts document frequencies while stemming and removing stopwords
	  *
	  * @param articles all parsed Wikipedia entries
	  * @return RDD of document frequencies
	  */
	def countDocumentFrequencies(
		articles: RDD[ParsedWikipediaEntry]
	): RDD[DocumentFrequency] = {
		val tokenizer = IngestionTokenizer(removeStopwords, stem)
		articles
			.flatMap(article => tokenizer.process(article.getText()).toSet)
			.map(word => (word, 1))
			.reduceByKey(_ + _)
			.map(DocumentFrequency.tupled)
	}

	/**
	  * Filters all document frequencies that do not meet the threshold
	  *
	  * @param documentFrequencies document frequencies to be filtered
	  * @param threshold           minimum amount of occurrences in documents
	  * @return filtered document frequencies
	  */
	def filterDocumentFrequencies(
		documentFrequencies: RDD[DocumentFrequency],
		threshold: Int
	): RDD[DocumentFrequency] = {
		documentFrequencies
			.filter(documentFrequency => documentFrequency.count >= threshold)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Document Frequency Counter")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val documentFrequencies = countDocumentFrequencies(allArticles)
		filterDocumentFrequencies(documentFrequencies, leastSignificantDocumentFrequency)
			.saveToCassandra(keyspace, outputDocumentFrequenciesTablename)

		sc.stop()
	}
}
