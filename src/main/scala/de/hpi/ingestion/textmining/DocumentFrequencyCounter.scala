package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{DocumentFrequency, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * Calculate document frequency over all articles
  */
object DocumentFrequencyCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputDocumentFrequenciesTablename = "wikipediadocfreq"
	val removeStopwords = true
	val stopwordsPath = "german_stopwords.txt"
	val leastSignificantDocumentFrequency = 5

	def stem(word: String): String = {
		val stemmer = new AccessibleGermanStemmer
		stemmer.stem(word)
	}

	def stemDocumentFrequencies(
		documentFrequencies: RDD[DocumentFrequency]
	): RDD[DocumentFrequency] = {
		documentFrequencies
			.map(df => (stem(df.word), df.count))
			.reduceByKey(_ + _)
			.map(DocumentFrequency.tupled)
	}

	def countDocumentFrequencies(
		articles: RDD[ParsedWikipediaEntry]
	): RDD[DocumentFrequency] = {
		val tokenizer = new CleanCoreNLPTokenizer
		articles
			.flatMap(article => tokenizer.tokenize(article.getText()).toSet)
			.map(word => (word, 1))
			.reduceByKey(_ + _)
			.map(DocumentFrequency.tupled)
	}

	def filterDocumentFrequencies(
		documentFrequencies: RDD[DocumentFrequency],
		threshold: Int,
		stopwords: Set[String]
	): RDD[DocumentFrequency] = {
		documentFrequencies
			.filter(documentFrequency => !stopwords.contains(documentFrequency.word) &&
				documentFrequency.count >= threshold)
	}

	def loadStopwords(): Set[String] = {
		var stopwords = Set[String]()
		if (removeStopwords) {
			stopwords = Source.fromURL(getClass.getResource(s"/$stopwordsPath"))
				.getLines()
				.toSet
		}
		stopwords
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Document Frequency Counter")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val stopwords = loadStopwords()
		val documentFrequencies = countDocumentFrequencies(allArticles)
		val filteredDocumentFrequencies = filterDocumentFrequencies(
			documentFrequencies,
			leastSignificantDocumentFrequency,
			stopwords
		)
		val stemmedDocumentFrequencies = stemDocumentFrequencies(filteredDocumentFrequencies)
		stemmedDocumentFrequencies
			.saveToCassandra(keyspace, outputDocumentFrequenciesTablename)

		sc.stop()
	}
}
