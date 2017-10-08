/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.preprocessing

import java.io.{BufferedReader, InputStream, InputStreamReader}
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.mutable

/**
  * Calculates the cosine similarity (context score) for each pair of `Link` alias and page it may refer to. It combines
  * it with the links score and entity score, which leads to the respective `FeatureEntry`.
  */
class CosineContextComparator extends SparkJob {
	import CosineContextComparator._
	appName = "Cosine Context Comparator"
	configFile = "textmining.xml"
	var aliasPageScores = Map.empty[String, List[(String, Double, Double)]]

	var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
	var aliases: RDD[Alias] = _
	var articleTfidf: RDD[ArticleTfIdf] = _
	var numDocuments: Long = 0
	var featureEntryList: List[RDD[FeatureEntry]] = Nil


	// $COVERAGE-OFF$
	var docFreqStreamFunction: String => InputStream = AliasTrieSearch.hdfsFileStream
	/**
	  * Loads Parsed Wikipedia entries with their term frequencies, the aliases with their pages and occurrences,
	  * the tfidf vectors for the Wikipedia articles and the total number of articles in the parsedwikipedia table
	  * from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		articleTfidf = sc.cassandraTable[ArticleTfIdf](settings("keyspace"), settings("tfidfTable"))
		aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		numDocuments = sc
			.cassandraTable[WikipediaArticleCount](settings("keyspace"), settings("articleCountTable"))
			.first
			.count
			.toLong
	}

	/**
	  * Saves the Feature Entries to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		featureEntryList.foreach(_.saveToCassandra(settings("keyspace"), settings("featureTable")))
	}
	// $COVERAGE-ON$

	/**
	  * Splits the input articles into 10 partitions which will be processed sequentially. Also removes any not needed
	  * data from the Parsed Wikipedia Entries.
	  * @return Collection of Parsed Wikipedia Entry RDDs to be processed sequentially.
	  */
	def splitInput(): List[RDD[ParsedWikipediaEntry]] = {
		parsedWikipedia
			.map(entry =>
				ParsedWikipediaEntry(
					title = entry.title,
					text = entry.text,
					linkswithcontext = entry.linkswithcontext,
					triealiases = entry.triealiases))
			.randomSplit(Array.fill(settings("numberArticlePartitions").toInt)(1.0))
			.toList
	}

	/**
	  * Calculates cosine similarity (context score) for every pair of link alias and page it may refer to.
	  * It combines it with the links score and entity score, which leads to a FeatureEntry.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		if(aliasPageScores.isEmpty) {
			aliasPageScores = collectAliasPageScores(aliases)
		}
		val tfidfTuples = articleTfidf.flatMap(ArticleTfIdf.unapply)
		val tokenizer = IngestionTokenizer(true, true)
		val articlesPartitions = splitInput()
		featureEntryList = articlesPartitions.map { articles =>
			val contextTfs = transformLinkContextTfs(articles, tokenizer, settings("contextSize").toInt)
			val contextTfidf = calculateTfidf(
				contextTfs,
				numDocuments,
				defaultIdf(numDocuments),
				docFreqStreamFunction,
				settings("dfFile"))
			compareLinksWithArticles(contextTfidf, sc.broadcast(aliasPageScores), tfidfTuples)
		}
	}
}

object CosineContextComparator {
	/**
	  * Computes scores for an alias being a link and referring to a specific page for all aliases.
	  *
	  * @param aliases aliases with their occurrence frequencies and pages they may refer to
	  * @return aliases with link score, pages and respective page scores
	  */
	def computeAliasPageScores(aliases: RDD[Alias]): RDD[(String, List[(String, Double, Double)])] = {
		aliases
			.filter(alias => alias.linkoccurrences.isDefined && alias.totaloccurrences.isDefined)
			.flatMap { alias =>
				val linkScore = alias.linkoccurrences.get.toDouble / alias.totaloccurrences.get.toDouble
				val numPages = alias.pages.values.sum.toDouble
				val pageScores = alias.pages.mapValues(_.toDouble / numPages)
				// Although linkScore is the same in each list entry, it is stored multiple times for easier processing.
				alias.pages.keySet.map(page => (alias.alias, List((page, linkScore, pageScores(page)))))
			}.reduceByKey(_ ++ _)
	}

	/**
	  * Computes scores for an Alias being a link and referring to a specific page for all aliases and collects the data
	  * into a Map.
	  * @param aliases aliases with their occurrence frequencies and pages they may refer to
	  * @return Map containing link score, pages and respective page scores for each alias
	  */
	def collectAliasPageScores(aliases: RDD[Alias]): Map[String, List[(String, Double, Double)]] = {
		computeAliasPageScores(aliases)
			.collect
			.toMap
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
	  * Calculates the inverse document frequency (idf) using the logarithm base 10 for a given Document Frequency and
	  * the number of documents used to calculate it.
	  *
	  * @param df      document frequency to use
	  * @param numDocs number of documents used to compute the document frequency
	  * @return calculated inverse document frequency
	  */
	def calculateIdf(df: Int, numDocs: Long): Double = {
		Math.log10(numDocs.toDouble / df.toDouble)
	}

	/**
	  * Calculates a default idf value for terms under the document frequency threshold.
	  * @param numDocs number of documents used
	  * @param dfThreshold minimum threshold for the document frequency
	  * @return default idf value
	  */
	def defaultIdf(
		numDocs: Long,
		dfThreshold: Int = DocumentFrequencyCounter.leastSignificantDocumentFrequency
	): Double = {
		calculateIdf(calculateDefaultDf(dfThreshold), numDocs)
	}

	/**
	  * Returns default value for the Document Frequency used in the idf calculation.
	  *
	  * @param threshold least significant Document Frequency
	  * @return default Document Frequency used for idf calculation
	  */
	def calculateDefaultDf(threshold: Int = DocumentFrequencyCounter.leastSignificantDocumentFrequency): Int = {
		threshold - 1
	}

	/**
	  * Calculates the tfIdf for every term.
	  *
	  * @param termFrequencies RDD of terms with their identifier and raw term frequency in the form
	  *                        (term, (identifier, raw term frequency))
	  * @param numDocuments number of documents used to determine the Document Frequencies
	  * @param defaultIdf default idf value for terms without a document frequency above the threshold
	  * @param docFreqStreamFunction function opening a hdfs file stream to a given file
	  * @param dfFile file containing the document frequencies
	  * @tparam T type of the identifier for each term
	  * @return RDD of tuples containing identifiers and a Map with the tfidf for each term
	  */
	def calculateTfidf[T](
		termFrequencies: RDD[(T, Bag[String, Int])],
		numDocuments: Long,
		defaultIdf: Double,
		docFreqStreamFunction: String => InputStream,
		dfFile: String
	): RDD[(T, Map[String, Double])] = {
		termFrequencies.mapPartitions({ partition =>
			val idfMap = inverseDocumentFrequencies(numDocuments, docFreqStreamFunction, dfFile)
			partition.map { case (identifier, bagOfWords) =>
				val tfidfMap = bagOfWords.getCounts().map { case (token, tf) =>
					val idf = idfMap.getOrElse(token, defaultIdf)
					val tfidf = tf * idf
					(token, tfidf)
				}
				(identifier, tfidfMap)
			}
		}, true)
	}

	/**
	  * Reads the document frequencies from the HDFS, calculates the inverse document frequencies and returns them
	  * as Map.
	  * @param numDocs number of documents used to create the document frequency data
	  * @param docFreqStreamFunction function opening a hdfs file stream to a given file
	  * @param dfFile file containing the document frequencies
	  * @return Map of every term and its inverse document frequency
	  */
	def inverseDocumentFrequencies(
		numDocs: Long,
		docFreqStreamFunction: String => InputStream,
		dfFile: String
	): Map[String, Double] = {
		val docfreqStream = docFreqStreamFunction(dfFile)
		val reader = new BufferedReader(new InputStreamReader(docfreqStream, "UTF-8"))
		val idfMap = mutable.Map[String, Double]()
		var line = reader.readLine()
		while(line != null) {
			val Array(count, token) = line.split("\t", 2)
			idfMap(token) = calculateIdf(count.toInt, numDocs)
			line = reader.readLine()
		}
		idfMap.toMap
	}

	/** Adds the cosine similarity feature value to the link and page score feature values.
	  *
	  * @param linkContexts    links with their contexts
	  * @param aliasPageScores link score and page score feature values
	  * @param articleContexts article names with their bags of words
	  * @return feature entries containing link scores, page scores and cosine similarity
	  */
	def compareLinksWithArticles(
		linkContexts: RDD[(Link, Map[String, Double])],
		aliasPageScores: Broadcast[Map[String, List[(String, Double, Double)]]],
		articleContexts: RDD[(String, Map[String, Double])]
	): RDD[FeatureEntry] = {
		linkContexts
			.mapPartitions({ partition =>
				val localAliases = aliasPageScores.value
				partition.flatMap { case (link, tfidfMap) =>
					localAliases.getOrElse(link.alias, Nil)
						.map { case (page, linkScore, pageScore) =>
							// Note: The page in the link is the actual target for the alias occurrence,
							// the joined page is just a possible target for the alias.
							(page, List(ProtoFeatureEntry(link, tfidfMap, linkScore, MultiFeature(pageScore))))
						}
				}
			}, true)
			.join(articleContexts)
			.flatMap { case (page, (protoFeatureEntries, articleContext)) =>
				protoFeatureEntries.map { protoFeatureEntry =>
					val cosineSim = calculateCosineSimilarity(protoFeatureEntry.linkContext, articleContext)
					protoFeatureEntry.toFeatureEntry(page, MultiFeature(cosineSim))
				}
			}
	}

	/**
	  * Calculates the length of a vector.
	  *
	  * @param vector vector
	  * @return length
	  */
	def calculateLength(vector: Iterable[Double]): Double = {
		math.sqrt(vector.map(t => t * t).sum)
	}

	/**
	  * Calculate the cosine similarity between two vectors with possibly different dimensions.
	  * Note: This value may become 0.0 if
	  * DocumentFrequencyCounter.leastSignificantDocumentFrequency > number of documents.
	  *
	  * @param vectorA vector with dimension keys
	  * @param vectorB vector with dimension keys
	  * @return cosine similarity
	  */
	def calculateCosineSimilarity[T](vectorA: Map[T, Double], vectorB: Map[T, Double]): Double = {
		val lengthA = calculateLength(vectorA.values)
		val lengthB = calculateLength(vectorB.values)
		val dotProduct = vectorA.keySet.intersect(vectorB.keySet)
			.map(key => vectorA(key) * vectorB(key))
			.sum
		val lengthProduct = lengthA * lengthB
		if(lengthProduct == 0) 0.0 else dotProduct / lengthProduct
	}

	/**
	  * Transforms articles into a format needed to calculate the tfIdf for every article.
	  *
	  * @param articles RDD of parsed Wikipedia articles with known term frequencies
	  * @return RDD of tuples containing the page name and the tfIdfs of every term on that page
	  */
	def transformArticleTfs(articles: RDD[ParsedWikipediaEntry]): RDD[(String, Bag[String, Int])] = {
		articles.map(entry => (entry.title, Bag.extract(entry.context)))
	}

	/**
	  * Transforms links into a format needed to calculate the tfIdf for every link.
	  *
	  * @param articles RDD of parsed Wikipedia articles containing the links and their contexts
	  * @param tokenizer tokenizer used to generate the contexts of the Trie Aliases
	  * @param contextSize number of tokens before and after a match considered as the context
	  * @return RDD of tuples containing the link and the tfIdfs of every term in its context
	  */
	def transformLinkContextTfs(
		articles: RDD[ParsedWikipediaEntry],
		tokenizer: IngestionTokenizer,
		contextSize: Int
	): RDD[(Link, Bag[String, Int])] = {
		articles
			.flatMap { entry =>
				val articleTokens = tokenizer.onlyTokenizeWithOffset(entry.getText())
				val trieAliasLinks = entry.triealiases.map { trieAlias =>
					val trieLink = trieAlias.toLink().copy(article = Option(entry.title))
					val context = TermFrequencyCounter.extractContext(
						articleTokens,
						trieAlias.toLink(),
						tokenizer,
						contextSize)
					(trieLink, context)
				}
				val links = entry.linkswithcontext.map { textLink =>
					val context = Bag.extract(textLink.context)
					(textLink.copy(article = Option(entry.title), context = Map()), context)
				}
				trieAliasLinks ++ links
			}
	}
}

