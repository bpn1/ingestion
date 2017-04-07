package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector

object WikipediaTfidfCalculator {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputTablename = "wikipediatfidf"
	val outputDirectoryTf = "tf.txt"
	val outputDirectoryTfidf = "tfidf.txt"

	def countTermFrequencies(articles: RDD[ParsedWikipediaEntry]): RDD[Vector] = {
		// https://spark.apache.org/docs/latest/mllib-feature-extraction.html
		val tokenizer = new CleanCoreNLPTokenizer
		val documents = articles
			.map(article => tokenizer.tokenize(article.getText()))
		val hashingTF = new HashingTF()
		hashingTF.transform(documents)
	}

	def calculateTfidf(termFrequencies: RDD[Vector]): RDD[Vector] = {
		// https://spark.apache.org/docs/latest/mllib-feature-extraction.html
		termFrequencies.cache()
		val idf = new IDF().fit(termFrequencies)
		val tfidf = idf.transform(termFrequencies)
		tfidf
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Term Frequency Counter")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputTablename)
		val termFrequencies = countTermFrequencies(allArticles)
		val arr = termFrequencies
			.collect
		arr.saveAsTextFile(outputDirectoryTf)
		calculateTfidf(termFrequencies)
			.collect
			.saveAsTextFile(outputDirectoryTfidf)

		sc.stop()
	}
}
