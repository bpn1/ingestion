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
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikidata entries, Parsed Wikipedia entries and Wikipedia pages from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = List(sc.cassandraTable[WikiDataEntity](settings("keyspace"), settings("wikidataTable")))
		val pages = List(sc.cassandraTable[Page](settings("keyspace"), settings("pageTable")))
		val articles = List(sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable")))
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
			.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Extracts Wikipedia page names of every tagged Wikidata entity.
	  * @param wikidata RDD of Wikidata entities
	  * @return RDD of Wikipedia article names
	  */
	def extractCompanyPages(wikidata: RDD[WikiDataEntity]): RDD[String] = {
		wikidata
			.filter(entry => entry.instancetype.isDefined && entry.wikiname.isDefined)
			.map(_.wikiname.get)
	}

	/**
	  * Extracts all aliases companies have.
	  * @param pages RDD of Wikipedia Pages containing all aliases of every page
	  * @param companyPages RDD containing the names of all company articles
	  * @return RDD containing all aliases companies have
	  */
	def extractCompanyAliases(pages: RDD[Page], companyPages: RDD[String]): RDD[String] = {
		val joinableComps = companyPages.map(t => Tuple2(t, t))
		pages
			.map(page => (page.page, page.aliases.keySet))
			.join(joinableComps)
			.flatMap { case (page, (aliases, page2)) => aliases }
			.distinct
	}

	/**
	  * Removes all Links of a Parsed Wikipedia entry whose alias is not in the List of company aliases.
	  * @param article Parsed Wikipedia entry to be cleaned
	  * @param companyAliases List of all aliases companies have
	  * @return Parsed Wikipedia entry with cleaned links
	  */
	def filterCompanyLinks(article: ParsedWikipediaEntry, companyAliases: Set[String]): ParsedWikipediaEntry = {
		article.reduceLinks(link => companyAliases.contains(link.alias))
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
		val companyAliases = extractCompanyAliases(pages, companyPages)
		val aliasBroadcast = sc.broadcast(companyAliases.collect.toSet)

		val filteredArticles = articles.mapPartitions({ rdd =>
			val localCompAliases = aliasBroadcast.value
			rdd.map(filterCompanyLinks(_, localCompAliases))
		}, true)
		List(filteredArticles).toAnyRDD()
	}
}
