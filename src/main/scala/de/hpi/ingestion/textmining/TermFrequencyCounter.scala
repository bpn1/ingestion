package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object TermFrequencyCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val inputDocumentFrequenciesTablename = "wikipediadocfreq"
	val outputArticlesTablename = "parsedwikipedia"
	val tokenizer = new CleanCoreNLPTokenizer
	val contextSize = 20

	/**
	  * Extracts the stemmed terms of a Wikipedia article and their relative frequency (between 0.0 and 1.0).
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return a Bag containing every term and its frequency
	  */
	def extractBagOfWords(entry: ParsedWikipediaEntry): Bag[String, Int] = {
		val tokens = tokenizer.tokenize(entry.getText())
		extractBagOfWords(tokens)
	}

	/**
	  * Enriches a Wikipedia article with its bag of words (implemented as Map)
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return parsed Wikipedia entry with additional context information
	  */
	def addArticleContext(entry: ParsedWikipediaEntry): ParsedWikipediaEntry = {
		var enrichedArticle = entry
		enrichedArticle.context = extractBagOfWords(entry).getCounts()
		enrichedArticle
	}

	/**
	  * Extracts the stemmed terms of a list of tokens and their relative frequency (between 0.0 and 1.0).
	  *
	  * @param tokens list of tokens to be counted
	  * @return a Bag containing every term and its frequency
	  */
	def extractBagOfWords(tokens: List[String]): Bag[String, Int] = {
		val stemmer = new AccessibleGermanStemmer
		val stemmedWords = tokens.map(stemmer.stem)
		Bag[String, Int](stemmedWords)
	}

	/**
	  * Transforms the term frequencies in a bag and an identifier for the term frequencies in a joinable format
	  * so that a join can be performed on the terms to calculate the tfIdf.
	  *
	  * @param identifier identifier for the terms contained in the bag
	  * @param tfBag      Bag containing the terms with their frequencies
	  * @tparam T type of the identifier (e.g. String for the article term frequencies and Link for the context
	  *           term frequencies.
	  * @return list of tuples with the structure (term, (identifier, term frequency))
	  */
	def joinableTermFrequencies[T](identifier: T, tfBag: Bag[String, Int]): List[(String, (T, Int))] = {
		tfBag.getCounts()
			.map { case (term, frequency) =>
				(term, (identifier, frequency))
			}.toList
	}

	/**
	  * Calculates the inverse document frequency (idf) for a given Document Frequency and the number of documents used
	  * to calculate it.
	  *
	  * @param df      document frequency to use
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
	  *
	  * @param df      document frequency to use
	  * @param numDocs number of documents used to compute the document frequency
	  * @return calculated inverse document frequency
	  */
	def calculateIdf(df: Int, numDocs: Long): Double = {
		assert(df > 0, s"Document frequency $df is not greater than 0.")
		Math.log10(numDocs.toDouble / df.toDouble)
	}

	/**
	  * Calculates the tfidf of every term for every Wikipedia article.
	  *
	  * @param articles                   RDD of parsed Wikipedia articles
	  * @param documentFrequencies        RDD of document frequencies
	  * @param numDocuments               number of documents that was used to calculate the document frequencies
	  * @param documentFrequencyThreshold lower threshold for document frequencies
	  * @return RDD of tuples containing the page name and the tfIdfs of every term on that page
	  */
	def calculateArticleTfidf(
		articles: RDD[ParsedWikipediaEntry],
		documentFrequencies: RDD[DocumentFrequency],
		numDocuments: Long,
		documentFrequencyThreshold: Int
	): RDD[(String, Map[String, Double])] = {
		val termFrequencies = articles
			.map(entry => (entry.title, extractBagOfWords(entry)))
			.flatMap(t => joinableTermFrequencies(t._1, t._2))

		calculateTfidf(
			termFrequencies,
			documentFrequencies,
			numDocuments,
			documentFrequencyThreshold)
			.reduceByKey(_ ++ _)
	}

	/**
	  * Calculates the tfIdf for the context of every link.
	  *
	  * @param articles                   RDD of parsed Wikipedia articles containing the links
	  * @param documentFrequencies        RDD of document frequencies
	  * @param numDocuments               number of documents that was used to calculate the document frequencies
	  * @param documentFrequencyThreshold lower threshold for document frequencies
	  * @return RDD of tuples containing the link and the tfIdfs of ever term in its context
	  */
	def calculateLinkContextsTfidf(
		articles: RDD[ParsedWikipediaEntry],
		documentFrequencies: RDD[DocumentFrequency],
		numDocuments: Long,
		documentFrequencyThreshold: Int
	): RDD[(Link, Map[String, Double])] = {
		val contextTermFrequencies = articles
			.flatMap(extractLinkContexts)
			.flatMap(t => joinableTermFrequencies(t._1, t._2))

		calculateTfidf(
			contextTermFrequencies,
			documentFrequencies,
			numDocuments,
			documentFrequencyThreshold)
			.reduceByKey(_ ++ _)
	}

	def calculateTfidf[T](
		termFrequencies: RDD[(String, (T, Int))],
		documentFrequencies: RDD[DocumentFrequency],
		numDocuments: Long,
		documentFrequencyThreshold: Int
	): RDD[(T, Map[String, Double])] = {
		val inverseDocumentFrequencies = documentFrequencies.map(calculateIdf(_, numDocuments))
		termFrequencies.leftOuterJoin(inverseDocumentFrequencies)
			.map { case (token, ((identifier, tf), idfOption)) =>
				val idf = idfOption.getOrElse(calculateIdf(documentFrequencyThreshold - 1, numDocuments))
				val tfidf = tf * idf
				(identifier, Map(token -> tfidf))
			}
	}

	/**
	  * Extracts context of the links of a given Wikipedia entry. The context are {@contextSize } tokens in front of
	  * and behind the location of the links alias in the tokenized text.
	  *
	  * @param entry Wikipedia entry containing the used links
	  * @return list of tuples containing the link and its context
	  */
	def extractLinkContexts(entry: ParsedWikipediaEntry): List[(Link, Bag[String, Int])] = {
		val tokens = tokenizer.tokenize(entry.getText())
		entry.allLinks()
			.map { link =>
				val aliasTokens = tokenizer.tokenize(link.alias)
				val aliasIndex = tokens.indexOfSlice(aliasTokens)
				val contextStart = Math.max(aliasIndex - contextSize, 0)
				val contextEnd = Math.min(aliasIndex + contextSize, tokens.length)
				val context = tokens.slice(contextStart, contextEnd)
				val bag = extractBagOfWords(context)
				(link, bag)
			}
	}

	/**
	  * Enriches links from all articles with their contexts (bag of words, implemented as Map)
	  *
	  * @param articles parsed Wikipedia articles which links will be enriched with their contexts
	  * @return links with additional context information
	  */
	def addLinksContexts(articles: RDD[ParsedWikipediaEntry]): RDD[ParsedWikipediaEntry] = {
		articles
			.map { article =>
				val links = extractLinkContexts(article)
					.map { case (link, bag) =>
						val enrichedLink = link
						enrichedLink.context = bag.getCounts()
						enrichedLink
					}
				val enrichedArticle = article
				enrichedArticle.linkswithcontext = links
				enrichedArticle
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Term Frequency Counter")

		val sc = new SparkContext(conf)
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val documentFrequencies = sc.cassandraTable[DocumentFrequency](keyspace, inputDocumentFrequenciesTablename)

		val articlesWithContext = articles
			.map(addArticleContext)

		addLinksContexts(articlesWithContext)
			.saveToCassandra(keyspace, outputArticlesTablename)

		//		val numDocuments = articles.count()
		//
		//		val articleTfidf = calculateArticleTfidf(
		//			articles,
		//			documentFrequencies,
		//			numDocuments,
		//			DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		//
		//		val contextTfidf = calculateLinkContextsTfidf(
		//			articles,
		//			documentFrequencies,
		//			numDocuments,
		//			DocumentFrequencyCounter.leastSignificantDocumentFrequency)

		sc.stop()
	}
}
