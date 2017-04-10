package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaTfidfContextExtractor {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val inputDocumentFrequenciesTablename = "wikipediadocfreq"
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
		PageTermFrequencies(article.title, bagOfWords.normalise())
	}

	def calculateInverseDocumentFrequencyValue(documentFrequency: Int, documents: Long): Double = {
		Math.log10(documents.toDouble / documentFrequency)
	}

	def calculateInverseDocumentFrequency(
		documentFrequency: DocumentFrequency,
		documents: Long
	): InverseDocumentFrequency = {
		InverseDocumentFrequency(
			documentFrequency.word,
			calculateInverseDocumentFrequencyValue(documentFrequency.count, documents)
		)
	}

	def calculateTfidfContexts(
		articles: RDD[ParsedWikipediaEntry],
		documentFrequencies: RDD[DocumentFrequency],
		documentFrequencyThreshold: Int
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
					case Some((token2, idf)) =>
						(page, token, tf, idf)
					case _ =>
						// compute default value
						val idf = calculateInverseDocumentFrequencyValue(documentFrequencyThreshold - 1, documents)
						(page, token, tf, idf)
				}
			}
			.map { case (page, token, tf, idf) => (page, token, tf * idf) }
			.groupBy { case (page, token, tfidf) => page }
			.map { case (page, pageTokens) =>
				val tokens = pageTokens
					.map { case (page1, token, tfidf) => (token, tfidf) }
					.toMap
				TfidfContext(page, tokens)
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
			.setAppName("Wikipedia Tfidf Context Extractor")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val documentFrequencies = sc.cassandraTable[DocumentFrequency](keyspace, inputDocumentFrequenciesTablename)
		calculateTfidfContexts(
			allArticles,
			documentFrequencies,
			WikipediaDocumentFrequencyCounter.leastSignificantDocumentFrequency
		).saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
