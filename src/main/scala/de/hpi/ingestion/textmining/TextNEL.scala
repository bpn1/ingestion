package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SplitSparkJob
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.AliasTrieSearch._
import de.hpi.ingestion.textmining.CosineContextComparator._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel
import com.datastax.spark.connector._

/**
  * Uses a trained classifier model to perform NER and NEL on Wikipedia articles.
  */
object TextNEL extends SplitSparkJob {
	appName = "Text NEL"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	var loadModelFunction: SparkContext => RandomForestModel = loadModel

	/**
	  * Loads Wikipedia entries and aliases from the Cassandra.
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
	  * Saves found links to named entities to the HDFS.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val namedEntities = output.head.asInstanceOf[RDD[Link]]
		namedEntities.saveToCassandra(settings("keyspace"), settings("namedEntitiesTable"))
	}

	/**
	  * Loads the random forest classifier model from the HDFS.
	  *
	  * @param sc SparkContext used for loading from the HDFS
	  * @return random forest classifier model
	  */
	def loadModel(sc: SparkContext): RandomForestModel = {
		RandomForestModel.load(sc, settings("classifierModel"))
	}

	// $COVERAGE-ON$

	/**
	  * Uses the trained classifier to find and link named entities.
	  *
	  * @param data       link candidates with their respective labeled points
	  * @param classifier random forest classifier model
	  * @return identified links to named entities
	  */
	def linkNamedEntities(
		data: RDD[(Link, LabeledPoint)],
		classifier: Broadcast[RandomForestModel]
	): RDD[Link] = {
		data.map { case (linkCandidate, LabeledPoint(label, dataPoint)) =>
			val prediction = classifier.value.predict(dataPoint)
			(linkCandidate, prediction)
		}.filter(_._2 == 1)
			.map(_._1)
			.map { link =>
				link.context = Map[String, Int]()
				link
			}
	}

	/**
	  * Finds all known aliases in a text.
	  *
	  * @param text             article text
	  * @param trie             trie with known aliases
	  * @param tokenizer        word tokenizer that splits text into raw words
	  * @param contextTokenizer word tokenizer for context extraction (may filter stopwords and stem)
	  * @return
	  */
	def matchTrieAliases(
		text: String,
		trie: TrieNode,
		tokenizer: IngestionTokenizer,
		contextTokenizer: IngestionTokenizer
	): List[TrieAlias] = {
		var trieAliases = List[TrieAlias]()
		val tokens = tokenizer.processWithOffsets(text)
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens.map(_.token))
			val offsetMatches = aliasMatches.map(textTokens => testTokens.take(textTokens.length))
			if(aliasMatches.nonEmpty) {
				val longestMatch = offsetMatches.maxBy(_.length)
				val offset = longestMatch.headOption.map(_.beginOffset)
				offset.foreach { begin =>
					val alias = text.substring(begin, longestMatch.last.endOffset)
					val context = extractAliasContext(longestMatch, tokens, i, contextTokenizer).getCounts()
					trieAliases :+= TrieAlias(alias, offset, context)
				}
			}
		}
		trieAliases
	}

	/**
	  * Finds candidates for links and extracts their term frequencies.
	  *
	  * @param articles RDD of parsed Wikipedia articles containing the links and their contexts
	  * @param aliases  RDD of all aliases
	  * @param sc       Spark Context used to broadcast variables
	  * @return RDD of tuples containing the link and the tfIdfs of ever term in its context
	  */
	def findLinkCandidates(
		articles: RDD[Article],
		aliases: RDD[Alias],
		sc: SparkContext
	): RDD[(Link, Bag[String, Int])] = {
		val tokenizer = IngestionTokenizer(false, false)
		val contextTokenizer = IngestionTokenizer(true, true)
		val aliasPagesMap = aliases
			.map(alias => (alias.alias, alias.pages.keys))
			.collect
			.toMap
		val settingsBroadcast = sc.broadcast(AliasTrieSearch.settings)

		articles.mapPartitions({ partition =>
			val localSettings = settingsBroadcast.value
			val trie = deserializeTrie(AliasTrieSearch.trieStreamFunction(localSettings("trieFile")))
			partition
				.flatMap { article =>
					val trieAliases = matchTrieAliases(article.text, trie, tokenizer, contextTokenizer)
					trieAliases.flatMap { trieAlias =>
						aliasPagesMap
							.getOrElse(trieAlias.alias, Nil)
							.map { page =>
								(Link(trieAlias.alias, page, trieAlias.offset, article = Option(article.title)),
									Bag(trieAlias.context))
							}
					}
				}
		}, true)
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
		CosineContextComparator.splitInput(input, args)
	}

	/**
	  * Uses a trained classifier model to perform NER and NEL on Wikipedia articles.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val allArticles = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val aliases = input(1).asInstanceOf[RDD[Alias]]
		val articlesPartition = input(2).asInstanceOf[RDD[ParsedWikipediaEntry]]

		val numDocuments = allArticles.count()
		val defaultDf = calculateDefaultDf(DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		val defaultIdf = calculateIdf(defaultDf, numDocuments)

		val articleTfs = transformArticleTfs(allArticles)
		val articleTfidf = calculateTfidf(articleTfs, numDocuments, defaultIdf)

		val articles = articlesPartition.map(article => Article(article.title, article.getText()))
		val linkCandidatesTfs = findLinkCandidates(articles, aliases, sc)
		val linkCandidatesTfidf = calculateTfidf(linkCandidatesTfs, numDocuments, defaultIdf)
		val aliasPageScores = computeAliasPageScores(aliases)
			.collect
			.toMap

		val linkFeatureEntries = CosineContextComparator
			.compareLinksWithArticles(linkCandidatesTfidf, sc.broadcast(aliasPageScores), articleTfidf)
		val data = linkFeatureEntries.map { case (link, featureEntry) => (link, featureEntry.labeledPoint()) }
		val classifier = sc.broadcast(loadModelFunction(sc))
		val namedEntities = linkNamedEntities(data, classifier)
		List(namedEntities).toAnyRDD()
	}
}
