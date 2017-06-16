package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.CosineContextComparator.{calculateDefaultDf, calculateIdf}
import de.hpi.ingestion.textmining.models.{Alias, ParsedWikipediaEntry, WikipediaArticleCount, FeatureEntry}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

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
		val articleCount = sc.cassandraTable[WikipediaArticleCount](settings("keyspace"), settings("articleCountTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		val wikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(spiegel, articleCount, aliases, wikipedia).flatMap(List(_).toAnyRDD())
	}

	/**
	  * Saves the generated Feature Entries to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
	    	.fromAnyRDD[FeatureEntry]()
	    	.head
	    	.saveToCassandra(settings("keyspace"), settings("spiegelFeatureEntryTable"))
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
		val spiegel = input.head.asInstanceOf[RDD[SpiegelArticle]].sample(false, 0.01)
		val numDocuments = input(1).asInstanceOf[RDD[WikipediaArticleCount]].collect.head.count.toLong
		val aliases = input(2).asInstanceOf[RDD[Alias]]
		val articles = input(3).asInstanceOf[RDD[ParsedWikipediaEntry]].map { entry =>
			ParsedWikipediaEntry(entry.title, context = entry.context)
		}

		val aliasPageScores = CosineContextComparator.computeAliasPageScores(aliases)
	    	.collect
	    	.toMap
		val aliasPageScoreBroadcast = sc.broadcast(aliasPageScores)

		val defaultDf = calculateDefaultDf(DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		val defaultIdf = calculateIdf(defaultDf, numDocuments)

		val tokenizer = IngestionTokenizer()
		val articleAliases = spiegel.flatMap { article =>
			val articleTokens = article.text.map(tokenizer.processWithOffsets).getOrElse(Nil)
			article.triealiases.map { triealias =>
				val context = TermFrequencyCounter.extractContext(articleTokens, triealias.toLink(), tokenizer)
				(triealias.toLink().copy(article = Option(article.id)), context)
			}
		}
		val aliasTfIdf = CosineContextComparator.calculateTfidf(articleAliases, numDocuments, defaultIdf)
		val articleTfIdf = CosineContextComparator.calculateTfidf(
			CosineContextComparator.transformArticleTfs(articles),
			numDocuments,
			defaultIdf)

		val featureEntries = CosineContextComparator.compareLinksWithArticles(
			aliasTfIdf,
			aliasPageScoreBroadcast,
			articleTfIdf)

		SecondOrderFeatureGenerator.run(List(featureEntries).toAnyRDD(), sc)
	}
}
