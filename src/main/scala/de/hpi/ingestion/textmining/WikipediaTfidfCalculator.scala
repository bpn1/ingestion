package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{Bag, DocumentFrequency, ParsedWikipediaEntry, TermFrequencies}
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
				val stemmedWords = tokenizer.tokenize(article.getText())
					.map(term => stemmer.stem(term))
				val bagOfWords = Bag[String, Int](stemmedWords)
				TermFrequencies(article.title, bagOfWords)
			}
	}

	def extractBagOfWords(
		entry: ParsedWikipediaEntry,
		tokenizer: Tokenizer,
		stemmer: AccessibleGermanStemmer
	): (String, Bag[String, Int]) = {
		val stemmedWords = tokenizer.tokenize(entry.getText())
			.map(stemmer.stem)
		val bagOfWords = Bag[String, Int](stemmedWords)
		(entry.title, bagOfWords)
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

	def calcTfidf(bag: Bag[String, Double], df: Map[String, Int], maxDf: Double): List[(String, Double)] = {
		bag.getCounts
			.filter(tf => df.contains(tf._1))
			.toList
			.map { case (word, count) =>
				val tfidf = Math.log(maxDf / df(word)) * count
				(word, tfidf)
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Term Frequency Counter")
		val sc = new SparkContext(conf)
		val parsedArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputTablename).cache
		val articleBags = parsedArticles
			.mapPartitions({ partition =>
				val tokenizer = new CleanCoreNLPTokenizer
				val stemmer = new AccessibleGermanStemmer
				partition.map(article => extractBagOfWords(article, tokenizer, stemmer))
			}, true)
			.collect
			.toMap

		val articleBagsBroadcast = sc.broadcast(articleBags)

		val df = sc.cassandraTable[DocumentFrequency](keyspace, "wikipediadocfreq")
			.map(df => (df.word, df.count))
			.collect
			.toMap

		val dfBroadcast = sc.broadcast(df)

		val maxDf = parsedArticles.count.toDouble
		parsedArticles
			.filter(_.title == "Audi")
			.flatMap(entry => entry.textlinks.map((entry.title, _)))
			.mapPartitions({ partition =>
				val dfLocal = dfBroadcast.value
				val articleBagsLocal = articleBagsBroadcast.value
				partition.map { case (title, link) =>
					val articleBag = articleBagsLocal(title).normalise
					val linkBag = articleBagsLocal(link.page).normalise
					val articleVector = calcTfidf(articleBag, dfLocal, maxDf)
					val linkVector = calcTfidf(linkBag, dfLocal, maxDf)
					val flattenLinks = linkVector.map((link, _))
					val flattenArticle = articleVector.map((title, _))
					var resultString = ""
					resultString += flattenArticle.map { tuple =>
						tuple._1 + "\t" + tuple._2._1 + "\t" + tuple._2._2
					}.mkString("\n")
					resultString += flattenLinks.map { tuple =>
						tuple._1.alias + "\t" + tuple._1.page + "\t" + tuple._2._1 + "\t" + tuple._2._2
					}.mkString("\n")
					resultString
				}
			}, true)
			.saveAsTextFile(s"tfidf_${System.currentTimeMillis}")

		sc.stop()
	}
}
