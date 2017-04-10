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
	val tokenizer = new CleanCoreNLPTokenizer

	/**
	  * Extracts the stemmed terms of a Wikipedia article and their relative frequency (between 0.0 and 1.0).
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @param tokenizer tokenizer used to tokenize the article
	  * @param stemmer stemmer used to stem the tokens
	  * @return tuple of the article title and a Bag containing every term and its frequency
	  */
	def extractBagOfWords(
		entry: ParsedWikipediaEntry,
		tokenizer: Tokenizer,
		stemmer: AccessibleGermanStemmer
	): (String, Bag[String, Double]) = {
		val tokens = tokenizer.tokenize(entry.getText())
		val stemmedWords = tokens.map(stemmer.stem)
		val bagOfWords = Bag[String, Int](stemmedWords)
		(entry.title, bagOfWords.normalise())
	}

	/**
	  * Calculates term frequency of every term in a given Wikipedia entry.
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return list of tuples with the structure (term, (page title, term frequency))
	  */
	def calculateTermFrequencies(entry: ParsedWikipediaEntry): List[(String, (String, Double))] = {
		val (title, wordBag) = extractBagOfWords(entry, tokenizer, new AccessibleGermanStemmer)
		wordBag.getCounts()
			.map { case (term, frequency) =>
				(term, (title, frequency))
			}.toList
	}

	/**
	  * Calculates the inverse document frequency (idf) for a given Document Frequency and the number of documents used
	  * to calculate it.
	  * @param df document frequency to use
	  * @param numDocs number of documents used to compute the document frequency
	  * @return tuple of term and calculated idf
	  */
	def calculateIdf(df: DocumentFrequency, numDocs: Long): (String, Double) = {
		val idf = calculateIdf(df.count, numDocs)
		(df.word, idf)
	}

	/**
	  * Calculates the inverse document frequency for a given document frequency and the number of documents used to
	  * calculate it.
	  * @param df document frequency to use
	  * @param numDocs number of documents used to compute the document frequency
	  * @return calculated inverse document frequency
	  */
	def calculateIdf(df: Int, numDocs: Long): Double = {
		assert(df > 0, s"Document frequency $df is not greater than 0.")
		Math.log10(numDocs.toDouble / df.toDouble)
	}

	/**
	  * Calculates the tfidf of every term for every Wikipedia article.
	  * @param articles RDD of parsed Wikipedia articles
	  * @param documentFrequencies RDD of document frequencies
	  * @param documentFrequencyThreshold lower threshold for document frequencies
	  * @return
	  */
	def calculateArticleTfidf(
		articles: RDD[ParsedWikipediaEntry],
		documentFrequencies: RDD[DocumentFrequency],
		documentFrequencyThreshold: Int
	): RDD[TfidfContext] = {
		val numDocuments = articles.count
		val inverseDocumentFrequencies = documentFrequencies.map(calculateIdf(_, numDocuments))
		val termFrequencies = articles.flatMap(calculateTermFrequencies)

		termFrequencies.leftOuterJoin(inverseDocumentFrequencies)
			.map { case (token, ((page, tf), idfOption)) =>
				val idf = idfOption.getOrElse(calculateIdf(documentFrequencyThreshold - 1, numDocuments))
				val tfidf = tf * idf
				(page, List((token, tfidf)))
			}.reduceByKey(_ ++ _)
			.map { case (page, pageTokens) =>
				TfidfContext(page, pageTokens.toMap)
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia tfIdf Context Extractor")

		val sc = new SparkContext(conf)
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val documentFrequencies = sc.cassandraTable[DocumentFrequency](keyspace, inputDocumentFrequenciesTablename)
		calculateArticleTfidf(
			articles,
			documentFrequencies,
			WikipediaDocumentFrequencyCounter.leastSignificantDocumentFrequency
		).saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
