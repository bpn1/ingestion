package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaTfidfCalculator {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputTablename = "wikipediatfidf"


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

	def countTermFrequencies(article: ParsedWikipediaEntry): PageTermFrequencies = {
		val tokenizer = new CleanCoreNLPTokenizer
		val stemmer = new AccessibleGermanStemmer
		val stemmedWords = tokenizer.tokenize(article.getText())
			.map(term => stemmer.stem(term))
		val bagOfWords = Bag[String, Int](stemmedWords)
		PageTermFrequencies(article.title, bagOfWords)
	}

	def calculateInverseDocumentFrequency(
		documentFrequency: DocumentFrequency,
		documents: Long
	): InverseDocumentFrequency = {
		InverseDocumentFrequency(documentFrequency.token, Math.log10(documents.toFloat / documentFrequency.count))
	}

	def calculateTfidfContexts(
		articles: RDD[ParsedWikipediaEntry],
		documentFrequencies: RDD[DocumentFrequency]
	): RDD[TfidfContext] = {
		val documents = articles.count
		val inverseDocumentFrequencies = documentFrequencies
			.map(documentFrequency => calculateInverseDocumentFrequency(documentFrequency, documents))
		articles
			.map(countTermFrequencies)
			.flatMap(ptf => ptf.tokens.getCounts().map(token => (ptf.page, token._1, token._2)))
			.keyBy { case (page, token, tf) => token }
			.leftOuterJoin {
				inverseDocumentFrequencies
					.map(idf => (idf.token, idf.value))
					.keyBy { case (token, value) => token }
			}
			.map { case (token, ((page, token1, tf), right)) =>
				right match {
					case Some((token2, idf)) => (page, token, tf, idf)
					case _ => (page, token, tf, 0.0) // todo: better default value
				}
			}
			.map { case (page, token, tf, idf) => (page, token, tf * idf) }
			.groupBy { case (page, token, tfidf) => page }
			.map { case (page, pageTokens) =>
				val tokens = pageTokens
					.map { case (page1, token, tfidf) => (token, tfidf) }
					.toMap
				TfidfContext(page, Bag(tokens))
			}
	}

	def calcTfidf(bag: Bag[String, Double], df: Map[String, Int], maxDf: Double): List[(String, Double)] = {
		bag.getCounts()
			.filter(tf => df.contains(tf._1))
			.toList
			.map {
				case (word, count) =>
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
			.mapPartitions({
				partition =>
					val tokenizer = new CleanCoreNLPTokenizer
					val stemmer = new AccessibleGermanStemmer
					partition.map(article => extractBagOfWords(article, tokenizer, stemmer))
			}, true)
			.collect
			.toMap

		val articleBagsBroadcast = sc.broadcast(articleBags)

		val df = sc.cassandraTable[DocumentFrequency](keyspace, "wikipediadocfreq")
			.map(df => (df.token, df.count))
			.collect
			.toMap

		val dfBroadcast = sc.broadcast(df)

		val maxDf = parsedArticles.count.toDouble
		parsedArticles
			.filter(_.title == "Audi")
			.flatMap(entry => entry.textlinks.map((entry.title, _)))
			.mapPartitions({
				partition =>
					val dfLocal = dfBroadcast.value
					val articleBagsLocal = articleBagsBroadcast.value
					partition.map {
						case (title, link) =>
							val articleBag = articleBagsLocal(title).normalise()
							val linkBag = articleBagsLocal(link.page).normalise()
							val articleVector = calcTfidf(articleBag, dfLocal, maxDf)
							val linkVector = calcTfidf(linkBag, dfLocal, maxDf)
							val flattenLinks = linkVector.map((link, _))
							val flattenArticle = articleVector.map((title, _))
							var resultString = ""
							resultString += flattenArticle.map {
								tuple =>
									tuple._1 + "\t" + tuple._2._1 + "\t" + tuple._2._2
							}.mkString("\n")
							resultString += flattenLinks.map {
								tuple =>
									tuple._1.alias + "\t" + tuple._1.page + "\t" + tuple._2._1 + "\t" + tuple._2._2
							}.mkString("\n")
							resultString
					}
			}, true)
			.saveAsTextFile(s"tfidf_${
				System.currentTimeMillis
			}")

		sc.stop()
	}
}
