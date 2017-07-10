package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.textmining.models._

object FeatureEntryGenerator extends SparkJob {
	appName = "Feature Entry Generator"
	configFile = "textmining.xml"
	sparkOptions("spark.yarn.executor.memoryOverhead") = "8192"

	// $COVERAGE-OFF$
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
		val wikidata = sc.cassandraTable[WikiDataEntity](settings("keyspace"), settings("wikidataTable"))
		List(tfArticles, aliases, articleTfidf, articleCount, wikidata).flatMap(List(_).toAnyRDD())
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
			.saveToCassandra(settings("keyspace"), "featureentries_testing2")
	}
	// $COVERAGE-ON$

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val List(articlesWithTermFrequencies, aliases, tfidf, articleCount, wikidata) = input
		val leanArticles = articlesWithTermFrequencies.asInstanceOf[RDD[ParsedWikipediaEntry]]
			.map(entry =>
				ParsedWikipediaEntry(
					title = entry.title,
					text = entry.text,
					linkswithcontext = entry.linkswithcontext,
					triealiases = entry.triealiases))
			.map(entry => Tuple2(entry.title, entry))
			val companyArticles = wikidata.asInstanceOf[RDD[WikiDataEntity]]
				.filter(_.instancetype.isDefined)
				.flatMap(_.wikiname)
				.map(name => (name, name))
				.join(leanArticles)
				.values
				.values
		val companyInput = List(tfidf, aliases) ++ List(companyArticles).toAnyRDD() ++ List(articleCount)
		CosineContextComparator.run(companyInput, sc, args)
	}
}
