package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SplitSparkJob
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

/**
  * Generates feature entries for all occurring pairs of aliases and pages they may refer to.
  */
object CosineContextComparator extends SplitSparkJob {
	appName = "Cosine Context Comparator"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries with their term frequencies and loads the document frequencies from the
	  * Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val tfArticles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		List(tfArticles).toAnyRDD() ++ List(aliases).toAnyRDD()
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
		dotProduct / (lengthA * lengthB)
	}

	/**
	  * Compares the context of a link and an article by comparing the tfidf values of the contexts with the cosine
	  * similarity.
	  * @param link Link to compare
	  * @param article Parsed Wikipedia Entry to compare
	  * @param dfData Document Frequency Data object containing the information about the document frequencies
	  * @return cosine similarity of the link and the article
	  */
	def compareContexts(
		link: Link,
		article: ParsedWikipediaEntry,
		dfData: DFData
	): Double = {
		val linkTfidf = link.context.map(dfData.idf)
		val articleTfidf = article.context.map(dfData.idf)
		calculateCosineSimilarity(linkTfidf, articleTfidf)
	}

	/**
	  * Computes probability that alias is a link and that it refers to a specific page for all aliases.
	  *
	  * @param aliases aliases with their occurrence frequencies and pages they may refer to
	  * @return aliases with probability features
	  */
	def computeAliasProbabilities(aliases: RDD[Alias]): RDD[(String, List[(String, Double, Double)])] = {
		aliases
			.filter(alias => alias.linkoccurrences.isDefined && alias.totaloccurrences.isDefined)
			.flatMap { alias =>
				val totalProb = alias.linkoccurrences.get.toDouble / alias.totaloccurrences.get.toDouble
				val numPages = alias.pages.values.sum.toDouble
				val pageProb = alias.pages.mapValues(_.toDouble / numPages)
				alias.pages.keySet.map(page => (alias.alias, List((page, totalProb, pageProb(page)))))
			}.reduceByKey(_ ++ _)
	}

	/**
	  * Enriches a Wikipedia article with its context as bag of words (implemented as Map).
	  *
	  * @param entry parsed Wikipedia entry which terms will be extracted
	  * @return parsed Wikipedia entry with additional context information
	  */
	def extractArticleContext(entry: ParsedWikipediaEntry, tokenizer: IngestionTokenizer): ParsedWikipediaEntry = {
		val tokens = tokenizer.process(entry.getText())
		val context = Bag.extract(tokens).getCounts()
		entry.copy(context = context)
	}

	/**
	  * Extracts context of the links and triealiases of a given Wikipedia entry. The contexts are n tokens in front of
	  * and behind the location of the links alias in the tokenized text. Extracts the links for the textlinks and the
	  * extendedlinks column.
	  *
	  * @param entry Wikipedia entry containing the used links
	  * @param tokenizer tokenizer to use to compute contexts
	  * @return Parsed Wikipedia Entry containing the enriched links and triealiases
	  */
	def extractLinkContexts(entry: ParsedWikipediaEntry, tokenizer: IngestionTokenizer): ParsedWikipediaEntry = {
		val contextSize = settings("contextSize").toInt
		val tokens = tokenizer.onlyTokenize(entry.getText())
		val (contextLinks, trieAliases) = (entry.textlinks ++ entry.extendedlinks() ++ entry.triealiases).map { link =>
			val alias = link match {
				case t: Link => t.alias
				case t: TrieAlias => t.alias
			}
			val aliasTokens = tokenizer.onlyTokenize(alias)
			val aliasIndex = tokens.indexOfSlice(aliasTokens)
			val contextStart = Math.max(aliasIndex - contextSize, 0)
			val preAliasContext = tokens.slice(contextStart, aliasIndex)
			val contextEnd = Math.min(aliasIndex + contextSize, tokens.length)
			val postAliasContext = tokens.slice(aliasIndex + aliasTokens.length, contextEnd)
			val context = preAliasContext ++ postAliasContext
			val bag = Bag.extract(tokenizer.process(context))
			link match {
				case t: Link => t.copy(context = bag.getCounts())
				case t: TrieAlias => t.copy(context = bag.getCounts())
			}
		}.partition(_.isInstanceOf[Link])
		entry.copy(
			linkswithcontext = contextLinks.asInstanceOf[List[Link]],
			triealiases = trieAliases.asInstanceOf[List[TrieAlias]])
	}

	/**
	  * Compares all links with the articles they might point to and generates Feature Entries used to train a
	  * classifier.
	  * @param articles RDD of Parsed Wikipedia Entries
	  * @param aliasPageProbabilities Map containing the pages and their link and page scores for every alias
	  * @param articleTexts Map containing the text of every article
	  * @param tokenizer tokenizer used to create the link and article contexts
	  * @param documentFrequencyData Document Frequency Data object containing the information about the document
	  *                              frequencies
	  * @return RDD of Article Feature Entries containing the generated Feature Entries for every article
	  */
	def compareLinksWithArticles(
		articles: RDD[ParsedWikipediaEntry],
		aliasPageProbabilities: Map[String, List[(String, Double, Double)]],
		articleTexts: Map[String, String],
		tokenizer: IngestionTokenizer,
		documentFrequencyData: DFData
	): RDD[ArticleFeatureEntry] = {
		articles
			.map(extractLinkContexts(_, tokenizer))
			.mapPartitions({ partition =>
				val idfMap = DFData.inverseDocumentFrequencies(documentFrequencyData.numDocuments)
				val dfData = documentFrequencyData.copy(idfMap = idfMap)
				partition.map { entry =>
					val featureEntries = entry.featureEntryLinks().flatMap { link =>
						val pages = aliasPageProbabilities.getOrElse(link.alias, Nil)
						pages.map { case (page, totalProb, pageProb) =>
							val linkedEntry = ParsedWikipediaEntry(page, articleTexts.get(page))
							val cosineSim = compareContexts(link, extractArticleContext(linkedEntry, tokenizer), dfData)
							FeatureEntry(link.alias, page, totalProb, pageProb, cosineSim, link.page == page)
						}
					}
					ArticleFeatureEntry(entry.title, featureEntries)
				}
			}, true)
	}

	/**
	  * Splits the input articles into 10 partitions which will be processed sequentially. Also removes any not needed
	  * data from the Parsed Wikipedia Entries.
	  * @param input List of RDDs containing the input data
	  * @param args arguments of the program
	  * @return Collection of input RDD Lists to be processed sequentially.
	  */
	override def splitInput(input: List[RDD[Any]], args: Array[String] = Array()): Traversable[List[RDD[Any]]] = {
		val List(articlesWithTermFrequencies, aliases) = input
		val leanArticles = articlesWithTermFrequencies.asInstanceOf[RDD[ParsedWikipediaEntry]]
			.map(_.copy(
				templatelinks = Nil,
				categorylinks = Nil,
				listlinks = Nil,
				disambiguationlinks = Nil,
				foundaliases = Nil))
		val weights = (0 until 10).map(t => 1.0).toArray
		val articleSplit = leanArticles.randomSplit(weights)
		articleSplit.map { articlePartition =>
			val leanerArticles = leanArticles.map(_.copy(triealiases = Nil))
			List(leanerArticles).toAnyRDD() ++ List(aliases).toAnyRDD() ++ List(articlePartition).toAnyRDD()
		}
	}

	/**
	  * Calculates the Cosine Similarities for every alias and creates feature entries containing the other two
	  * features as well.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articlesWithTermFrequencies = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val aliases = input(1).asInstanceOf[RDD[Alias]]
		val linkArticles = input(2).asInstanceOf[RDD[ParsedWikipediaEntry]]

		val numDocuments = articlesWithTermFrequencies.count()
		val defaultDf = DFData.defaultDF(DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		val defaultIdf = DFData.calculateIdf(defaultDf, numDocuments)
		val dfData = DFData(numDocuments, defaultIdf)

		val aliasPageProbabilities = computeAliasProbabilities(aliases)
			.collect
			.toMap
		val articleTexts = articlesWithTermFrequencies
			.map(entry => (entry.title, entry.getText()))
			.collect
			.toMap

		val tokenizer = IngestionTokenizer(args)
		val featureEntries = compareLinksWithArticles(
			linkArticles,
			aliasPageProbabilities,
			articleTexts,
			tokenizer,
			dfData)
		List(featureEntries).toAnyRDD()
	}
}
