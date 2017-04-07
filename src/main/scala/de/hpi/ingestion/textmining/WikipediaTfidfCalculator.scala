package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{ParsedWikipediaEntry, Bag}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaTfidfCalculator {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputTablename = "wikipediatfidf"
	val outputDirectoryTf = "tf.txt"
	val outputDirectoryTfidf = "tfidf.txt"

	def articlesToBagsOfWords(articles: RDD[ParsedWikipediaEntry]): RDD[Bag[String, Double]] = {
		val tokenizer = new CleanCoreNLPTokenizer
		articles
			.map(article => Bag(tokenizer.tokenize(article.getText())))
	}

	//	def countTermFrequencies(articles: RDD[ParsedWikipediaEntry]): RDD[Vector] = {
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
		//		val termFrequencies = countTermFrequencies(allArticles)
		//		calculateTfidf(termFrequencies)
		//			.saveAsTextFile(outputDirectoryTfidf)

		sc.stop()
	}
}
