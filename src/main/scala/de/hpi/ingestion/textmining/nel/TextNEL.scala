package de.hpi.ingestion.textmining.nel

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SplitSparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.CosineContextComparator.{calculateTfidf, compareLinksWithArticles, defaultIdf}
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import de.hpi.ingestion.textmining.{CosineContextComparator, SecondOrderFeatureGenerator, TermFrequencyCounter}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

/**
  * Classifies the Trie Aliases found in Articles by the Article Trie Search and writes the positives as found entities
  * to the same table.
  */
object TextNEL extends SplitSparkJob {
	appName = "Text NEL"
	configFile = "textmining.xml"
	sparkOptions("spark.yarn.executor.memoryOverhead") = "4096"

	// $COVERAGE-OFF$
	/**
	  * Loads the Trie Alias Articles from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val spiegel = sc.cassandraTable[TrieAliasArticle](settings("keyspace"), settings("NELTable"))
//			.sample(false, 0.001)
		val articleCount = sc.cassandraTable[WikipediaArticleCount](settings("keyspace"), settings("articleCountTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		val articleTfidf = sc.cassandraTable[ArticleTfIdf](settings("keyspace"), settings("tfidfTable"))
		List(spiegel, articleCount, aliases, articleTfidf).flatMap(List(_).toAnyRDD())
	}

	/**
	  * Saves the found entities to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[(String, List[Link])]()
			.head
			.saveToCassandra(
				settings("keyspace"),
				settings("NELTable"),
				SomeColumns("article", "foundentities"))
	}
	// $COVERAGE-ON$

	override def splitInput(input: List[RDD[Any]], args: Array[String]): Traversable[List[RDD[Any]]] = {
		val List(articles, numDocuments, aliases, wikipediaTfidf) = input
		articles
			.randomSplit(Array.fill(20)(1.0))
			.map(List(_, numDocuments, aliases, wikipediaTfidf))
	}

	/**
	  * Generates Feature Entries for the Aliases extraced from the Trie Alias Articles.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = input.head.asInstanceOf[RDD[TrieAliasArticle]]
		val numDocuments = input(1).asInstanceOf[RDD[WikipediaArticleCount]].collect.head.count.toLong
		val aliases = input(2).asInstanceOf[RDD[Alias]]
		val wikipediaTfidf = input(3).asInstanceOf[RDD[ArticleTfIdf]].flatMap(ArticleTfIdf.unapply)

		val aliasPageScores = CosineContextComparator.collectAliasPageScores(aliases)
		val tokenizer = IngestionTokenizer()

		val articleAliases = articles.flatMap { article =>
			val articleTokens = article.text.map(tokenizer.processWithOffsets).getOrElse(Nil)
			article.triealiases.map { triealias =>
				val context = TermFrequencyCounter.extractContext(articleTokens, triealias.toLink(), tokenizer)
				(triealias.toLink().copy(article = Option(article.article)), context)
			}
		}
		val aliasTfIdf = calculateTfidf(articleAliases, numDocuments, defaultIdf(numDocuments))

		val featureEntries = compareLinksWithArticles(aliasTfIdf, sc.broadcast(aliasPageScores), wikipediaTfidf)
		val extendedFeatureEntries = SecondOrderFeatureGenerator.run(List(featureEntries).toAnyRDD(), sc)
			.fromAnyRDD[FeatureEntry]().head

		val modelBroadcast = sc.broadcast(RandomForestModel.load(sc, settings("classifierModel")))
		val foundEntities = extendedFeatureEntries.mapPartitions({ partition =>
			val model = modelBroadcast.value
			partition.map { featureEntry =>
				(featureEntry, model.predict(featureEntry.labeledPoint().features))
			}
		}).collect {
			case (featureEntry, prediction) if prediction == 1.0 =>
				val link = Link(featureEntry.alias, featureEntry.entity, Option(featureEntry.offset))
				(featureEntry.article, List(link))
		}.reduceByKey(_ ++ _)
		List(foundEntities).toAnyRDD()
	}
}
