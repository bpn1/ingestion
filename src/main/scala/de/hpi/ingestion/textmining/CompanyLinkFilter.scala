package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Page, ParsedWikipediaEntry}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity

/**
  * Removes all Wikipedia links with an alias not having the possibility to point to a company.
  */
object CompanyLinkFilter extends SparkJob {
	appName = "Company Link Filter"
	val keyspace = "wikidumps"
	val wikidataTable = "wikidata"
	val articleTable = "parsedwikipedia"
	val pageTable = "wikipediapages"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikidata entries, Parsed Wikipedia entries and Wikipedia pages from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = List(sc.cassandraTable[WikiDataEntity](keyspace, wikidataTable))
		val pages = List(sc.cassandraTable[Page](keyspace, pageTable))
		val articles = List(sc.cassandraTable[ParsedWikipediaEntry](keyspace, articleTable))
		wikidata.toAnyRDD() ++ pages.toAnyRDD() ++ articles.toAnyRDD()
	}

	/**
	  * Saves cleaned Parsed Wikipedia entries to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.saveToCassandra(keyspace, articleTable)
	}
	// $COVERAGE-ON$

	def extractCompanyPages(wikidata: RDD[WikiDataEntity]): RDD[String] = {
		wikidata
			.filter(entry => entry.instancetype.isDefined && entry.wikiname.isDefined)
			.map(_.wikiname.get)
	}

	def extractCompanyAliases(sc: SparkContext, pages: RDD[Page], companyPages: RDD[String]): RDD[String] = {
		val joinableComps = companyPages.map(t => Tuple2(t, t))
		pages
			.map(page => (page.page, page.aliases.keySet))
			.join(joinableComps)
			.flatMap { case (page, (aliases, page2)) => aliases }
			.distinct
	}

	def filterCompanyLinks(article: ParsedWikipediaEntry, companyAliases: List[String]): ParsedWikipediaEntry = {
		article.filterLinks(link => companyAliases.contains(link.alias))
		article
	}

	/**
	  * Removes all non company links from the Parsed Wikipedia entries.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = input.head.asInstanceOf[RDD[WikiDataEntity]]
		val pages = input(1).asInstanceOf[RDD[Page]]
		val articles = input(2).asInstanceOf[RDD[ParsedWikipediaEntry]]

		val companyPages = extractCompanyPages(wikidata)
		val companyAliases = extractCompanyAliases(sc, pages, companyPages)
		val aliasBroadcast = sc.broadcast(companyAliases.collect.toList)

		val filteredArticles = articles.mapPartitions({ rdd =>
			val localCompAliases = aliasBroadcast.value
			rdd.map(filterCompanyLinks(_, localCompAliases))
		}, true)
		List(filteredArticles).toAnyRDD()
	}
}
