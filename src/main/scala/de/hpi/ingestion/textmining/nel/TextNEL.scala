package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.framework.SplitSparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.textmining.CosineContextComparator.{calculateTfidf, compareLinksWithArticles, defaultIdf}
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import de.hpi.ingestion.textmining.{CosineContextComparator, SecondOrderFeatureGenerator, TermFrequencyCounter}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Classifies the Trie Aliases found in Articles by the Article Trie Search and writes the positives as found entities
  * to the same table.
  */
object TextNEL extends SplitSparkJob {
	appName = "Text NEL"
	configFile = "textmining.xml"
	sparkOptions("spark.yarn.executor.memoryOverhead") = "4096"
	var aliasPageScores = Map.empty[String, List[(String, Double, Double)]]

	// $COVERAGE-OFF$
	var loadModelFunction: SparkContext => RandomForestModel = loadModel _

	/**
	  * Loads the Trie Alias Articles from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[TrieAliasArticle](settings("keyspace"), settings("NELTable"))
		val articleCount = sc.cassandraTable[WikipediaArticleCount](settings("keyspace"), settings("articleCountTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		val articleTfidf = sc.cassandraTable[ArticleTfIdf](settings("keyspace"), settings("tfidfTable"))
		List(articles, articleCount, aliases, articleTfidf).flatMap(List(_).toAnyRDD())
	}

	/**
	  * Saves the found entities to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[(String, List[Link])]()
			.head
			.saveToCassandra(
				settings("keyspace"),
				settings("NELTable"),
				SomeColumns("id", "foundentities"))
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
	  * Splits the Articles into n parts.
	  *
	  * @param input List of RDDs containing the input data
	  * @param args  arguments of the program
	  * @return Collection of input RDD Lists to be processed sequentially.
	  */
	override def splitInput(input: List[RDD[Any]], args: Array[String] = Array()): Traversable[List[RDD[Any]]] = {
		val List(articles, numDocuments, aliases, wikipediaTfidf) = input
		val splitMap = Map("wikipedianel" -> 100, "spiegel" -> 20)
		val splitCount = splitMap.getOrElse(settings("NELTable"), 20)
		articles
			.randomSplit(Array.fill(splitCount)(1.0))
			.map(List(_, numDocuments, aliases, wikipediaTfidf))
	}

	/**
	  * Extracts the contexts of an article's Trie Aliases.
	  *
	  * @param article   article containing the text and the Trie Aliases.
	  * @param tokenizer Tokenizer used to create the contexts
	  * @return List of the Trie Aliases as Links and their contexts as Bags as tuples
	  */
	def extractTrieAliasContexts(
		article: TrieAliasArticle,
		tokenizer: IngestionTokenizer
	): List[(Link, Bag[String, Int])] = {
		val articleTokens = article.text.map(tokenizer.onlyTokenizeWithOffset).getOrElse(Nil)
		article.triealiases.map { triealias =>
			val context = TermFrequencyCounter.extractContext(articleTokens, triealias.toLink(), tokenizer)
			(triealias.toLink().copy(article = Option(article.id)), context)
		}
	}

	/**
	  * Uses a Random Forest Model to classify the given Feature Entries and transforms them into Links.
	  *
	  * @param featureEntries RDD of Feature Entries to classify
	  * @param model          broadcast of the Random Forest Model
	  * @return RDD of tuples containing the article and the entities linked in the article
	  */
	def classifyFeatureEntries(
		featureEntries: RDD[FeatureEntry],
		model: Broadcast[RandomForestModel]
	): RDD[(String, List[Link])] = {
		featureEntries
			.mapPartitions({ partition =>
				val localModel = model.value
				partition.map { featureEntry =>
					(featureEntry, localModel.predict(featureEntry.labeledPoint().features))
				}
			}).collect {
				case (featureEntry, prediction) if prediction == 1.0 =>
					val link = Link(featureEntry.alias, featureEntry.entity, Option(featureEntry.offset))
					(featureEntry.article, List(link))
			}.reduceByKey(_ ++ _)
			.map(_.map(identity, _.sortBy(_.offset)))
	}

	/**
	  * Add missing article IDs to found entities so that the foundentities column in the Cassandra does not stay null.
	  *
	  * @param foundEntities RDD of tuples containing the article and the entities linked in the article
	  * @param allArticleIds IDs of all articles (in split partition)
	  * @return found entities for all articles
	  */
	def addMissingArticleIds(
		foundEntities: RDD[(String, List[Link])],
		allArticleIds: RDD[String]
	): RDD[(String, List[Link])] = {
		allArticleIds
			.map(id => (id, id))
			.leftOuterJoin(foundEntities)
			.values
			.map { case (id, links) => (id, links.getOrElse(Nil)) }
	}

	/**
	  * Generates Feature Entries for the Aliases extracted from the Trie Alias Articles.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articlesPartition = input.head.asInstanceOf[RDD[TrieAliasArticle]]
		val numDocuments = input(1).asInstanceOf[RDD[WikipediaArticleCount]].collect.head.count.toLong
		val aliases = input(2).asInstanceOf[RDD[Alias]]
		val wikipediaTfidf = input(3).asInstanceOf[RDD[ArticleTfIdf]].flatMap(ArticleTfIdf.unapply)

		if(aliasPageScores.isEmpty) {
			aliasPageScores = CosineContextComparator.collectAliasPageScores(aliases)
		}
		val tokenizer = IngestionTokenizer()
		val articleAliases = articlesPartition.flatMap(extractTrieAliasContexts(_, tokenizer))
		val aliasTfIdf = calculateTfidf(articleAliases, numDocuments, defaultIdf(numDocuments))
		val featureEntries = compareLinksWithArticles(aliasTfIdf, sc.broadcast(aliasPageScores), wikipediaTfidf)
		val extendedFeatureEntries = SecondOrderFeatureGenerator
			.run(List(featureEntries).toAnyRDD(), sc)
			.fromAnyRDD[FeatureEntry]()
			.head
		val foundEntities = classifyFeatureEntries(extendedFeatureEntries, sc.broadcast(loadModelFunction(sc)))
		val articleIds = articlesPartition.map(_.id)
		val foundEntitiesForAllArticles = addMissingArticleIds(foundEntities, articleIds)

		List(foundEntitiesForAllArticles).toAnyRDD()
	}
}
