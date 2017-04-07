package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{Bag, ParsedWikipediaEntry, TermFrequencies}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaTfidfCalculator {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputTablename = "wikipediatfidf"

	def articlesToBagsOfWords(articles: RDD[ParsedWikipediaEntry]): RDD[TermFrequencies] = {
		val tokenizer = new CleanCoreNLPTokenizer
		val stemmer = new AccessibleGermanStemmer
		articles
			.map { article =>
				val listOfStemmedWords = tokenizer.tokenize(article.getText())
					.map(term => stemmer.stem(term))
				val bagOfWords = Bag[String, Int](listOfStemmedWords)
				TermFrequencies(article.title, bagOfWords)
			}
	}

	//	def countTermFrequencies(articles: RDD[ParsedWikipediaEntry]): RDD[Bag[String, Double]] = {
	//		val tokenizer = new CleanCoreNLPTokenizer
	//		val documents = articles
	//			.map(article => tokenizer.tokenize(article.getText()))
	//
	//	}

	//
	//	def calculateTfidf(termFrequencies: RDD[Vector]): RDD[Vector] = {
	//
	//	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Term Frequency Counter")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputTablename)
		val tf = articlesToBagsOfWords(allArticles)
		//		val termFrequencies = countTermFrequencies(allArticles)
		//		calculateTfidf(termFrequencies)
		//			.saveAsTextFile(outputDirectoryTfidf)

		sc.stop()
	}
}
