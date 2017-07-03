package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.framework.SplitSparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import com.datastax.spark.connector._
import scala.collection.mutable
import java.io.{BufferedReader, InputStream, InputStreamReader}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.broadcast.Broadcast

/**
  * Generates feature entries for all occurring pairs of aliases and pages they may refer to.
  */
object CosineContextComparator extends SplitSparkJob {
	appName = "Cosine Context Comparator"
	configFile = "textmining.xml"
	var aliasPageScores = Map.empty[String, List[(String, Double, Double)]]

	// $COVERAGE-OFF$
	var docFreqStreamFunction: String => InputStream = AliasTrieSearch.hdfsFileStream _

	/**
	  * Loads Parsed Wikipedia entries with their term frequencies, the aliases with their pages and occurrences,
	  * the tfidf vectors for the Wikipedia articles and the total number of articles in the parsedwikipedia table
	  * from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val tfArticles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		val articleTfidf = sc.cassandraTable[ArticleTfIdf](settings("keyspace"), settings("tfidfTable"))
		val articleCount = sc.cassandraTable[WikipediaArticleCount](settings("keyspace"), settings("articleCountTable"))
		List(tfArticles, aliases, articleTfidf, articleCount).flatMap(List(_).toAnyRDD())
	}

	/**
	  * Saves the Feature Entries to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[FeatureEntry]()
			.head
			.saveToCassandra(settings("keyspace"), settings("featureTable"))
	}
	// $COVERAGE-ON$

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
	  * @return RDD of tuples containing the link and the tfIdfs of every term in its context
	  */
	def transformLinkContextTfs(
		articles: RDD[ParsedWikipediaEntry],
		tokenizer: IngestionTokenizer
	): RDD[(Link, Bag[String, Int])] = {
		articles
			.flatMap { entry =>
				val articleTokens = tokenizer.onlyTokenizeWithOffset(entry.getText())
				val trieAliasLinks = entry.triealiases.map { trieAlias =>
					val trieLink = trieAlias.toLink().copy(article = Option(entry.title))
					val context = TermFrequencyCounter.extractContext(articleTokens, trieAlias.toLink(), tokenizer)
					(trieLink, context)
				}
				val links = entry.linkswithcontext.map { textLink =>
					val context = Bag.extract(textLink.context)
					(textLink.copy(article = Option(entry.title), context = Map()), context)
				}
				trieAliasLinks ++ links
			}
	}

	/**
	  * Calculates the tfIdf for every term.
	  *
	  * @param termFrequencies RDD of terms with their identifier and raw term frequency in the form
	  *                        (term, (identifier, raw term frequency))
	  * @param numDocuments    number of documents used to determine the Document Frequencies
	  * @param defaultIdf      default idf value for terms without a document frequency above the threshold
	  * @tparam T type of the identifier for each term
	  * @return RDD of tuples containing identifiers and a Map with the tfidf for each term
	  */
	def calculateTfidf[T](
		termFrequencies: RDD[(T, Bag[String, Int])],
		numDocuments: Long,
		defaultIdf: Double
	): RDD[(T, Map[String, Double])] = {
		termFrequencies.mapPartitions({ partition =>
			val idfMap = inverseDocumentFrequencies(numDocuments)
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
	  * Reads the document frequencies from the HDFS, calculates the inverse document frequencies and returns them
	  * as Map.
	  *
	  * @param numDocs number of documents used to create the document frequency data
	  * @return Map of every term and its inverse document frequency
	  */
	def inverseDocumentFrequencies(numDocs: Long): Map[String, Double] = {
		val docfreqStream = docFreqStreamFunction(settings("dfFile"))
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

	/**
	  * Splits the input articles into 10 partitions which will be processed sequentially. Also removes any not needed
	  * data from the Parsed Wikipedia Entries.
	  *
	  * @param input List of RDDs containing the input data
	  * @param args  arguments of the program
	  * @return Collection of input RDD Lists to be processed sequentially.
	  */
	override def splitInput(input: List[RDD[Any]], args: Array[String] = Array()): Traversable[List[RDD[Any]]] = {
		val List(articlesWithTermFrequencies, aliases, tfidf, articleCount) = input
		articlesWithTermFrequencies.asInstanceOf[RDD[ParsedWikipediaEntry]]
			.map(entry =>
				ParsedWikipediaEntry(
					title = entry.title,
					text = entry.text,
					linkswithcontext = entry.linkswithcontext,
					triealiases = entry.triealiases))
			.randomSplit(Array.fill(settings("numberArticlePartitions").toInt)(1.0))
			.map { articlePartition =>
				List(tfidf, aliases) ++ List(articlePartition).toAnyRDD() ++ List(articleCount)
			}
	}

	/**
	  * Calculates cosine similarities for every alias and creates feature entries containing the link and page scores
	  * features as well.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articleTfidf = input.head.asInstanceOf[RDD[ArticleTfIdf]].flatMap(ArticleTfIdf.unapply)
		val aliases = input(1).asInstanceOf[RDD[Alias]]
		val linkArticles = input(2).asInstanceOf[RDD[ParsedWikipediaEntry]]
		val numDocuments = input(3).asInstanceOf[RDD[WikipediaArticleCount]].collect.head.count.toLong
		if(aliasPageScores.isEmpty) {
			aliasPageScores = collectAliasPageScores(aliases)
		}

		val tokenizer = IngestionTokenizer(true, true)
		val contextTfs = transformLinkContextTfs(linkArticles, tokenizer)
		val contextTfidf = calculateTfidf(contextTfs, numDocuments, defaultIdf(numDocuments))

		val featureEntries = compareLinksWithArticles(contextTfidf, sc.broadcast(aliasPageScores), articleTfidf)
		SecondOrderFeatureGenerator.run(List(featureEntries).toAnyRDD(), sc, args)
	}
}
