package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.CosineContextComparator.{calculateTfidf, compareLinksWithArticles, defaultIdf}
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.mllib.tree.model.RandomForestModel

object SpiegelNEL extends SparkJob {
	appName = "Spiegel NEL"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the Spiegel Articles from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val spiegel = sc.cassandraTable[SpiegelArticle](settings("keyspace"), settings("spiegelTable"))
			.sample(false, 0.001)
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
				settings("spiegelTable"),
				SomeColumns("id", "foundentities"))
	}
	// $COVERAGE-ON$

	/**
	  * Generates Feature Entries for the Aliases extraced from the Spiegel entries.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val spiegel = input.head.asInstanceOf[RDD[SpiegelArticle]]
		val numDocuments = input(1).asInstanceOf[RDD[WikipediaArticleCount]].collect.head.count.toLong
		val aliases = input(2).asInstanceOf[RDD[Alias]]
		val wikipediaTfidf = input(3).asInstanceOf[RDD[ArticleTfIdf]].flatMap(ArticleTfIdf.unapply)

		val aliasPageScores = CosineContextComparator.collectAliasPageScores(aliases)
		val tokenizer = IngestionTokenizer()

		val articleAliases = spiegel.flatMap { article =>
			val articleTokens = article.text.map(tokenizer.processWithOffsets).getOrElse(Nil)
			article.triealiases.map { triealias =>
				val context = TermFrequencyCounter.extractContext(articleTokens, triealias.toLink(), tokenizer)
				(triealias.toLink().copy(article = Option(article.id)), context)
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
