package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.ParsedWikipediaEntry
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector

object WikipediaTermFrequencyCounter {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputTablename = "wikipediatermfreq"

	def countTermFrequencies(articles: RDD[ParsedWikipediaEntry]): RDD[Vector] = {
		val tokenizer = new CleanCoreNLPTokenizer
		val documents = articles
			.map(article => tokenizer.tokenize(article.getText()))
		val hashingTF = new HashingTF()
		hashingTF.transform(documents)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Term Frequency Counter")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, outputTablename)
		countTermFrequencies(allArticles)
			.saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
