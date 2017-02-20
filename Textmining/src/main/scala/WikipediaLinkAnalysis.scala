import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import WikiClasses._

object WikipediaLinkAnalysis {
	val keyspace = "wikidumps"
	val inputRawTablename = "wikipedia"
	val inputParsedTablename = "parsedwikipedia"
	val outputAliasToPagesTablename = "wikipedialinks"
	val outputPageToAliasesTablename = "wikipediapages"

	def getAllPages(sc: SparkContext): RDD[String] = {
		sc.cassandraTable[WikipediaEntry](keyspace, inputRawTablename)
			.map(_.title)
	}

	def removeDeadLinks(links: RDD[Alias], allPages: RDD[String]): RDD[Alias] = {
		links
			.flatMap { link =>
				link.pages
					.map(page => (link.alias, page._1, page._2))
			}
			.keyBy { case (alias, pageName, count) => pageName }
			.join(allPages.map(entry => (entry, entry)).keyBy(_._1)) // ugly, but working
			.map { case (pageName, (link, doublePageName)) => link }
			.map { case (alias, pageName, count) => (alias, Seq((pageName, count))) }
			.reduceByKey(_ ++ _)
			.map { case (alias, links) => Alias(alias, links) }
	}

	def removeDeadPages(pages: RDD[Page], allPages: RDD[String]): RDD[Page] = {
		pages
			.keyBy(_.page)
			.join(allPages.map(entry => (entry, entry)).keyBy(_._1))
			.map { case (pageName, (page, doublePageName)) => page }
	}

	def groupByAliases(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Alias] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.alias, link.page))
			.groupByKey
			.map { case (alias, pageList) =>
				val pages = pageList
					.groupBy(identity)
					.mapValues(_.size)
					.toSeq
				Alias(alias, pages)
			}
	}

	def groupByPageNames(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Page] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.page, link.alias))
			.groupByKey
			.map { case (page, aliasList) =>
				val aliases = aliasList
					.groupBy(identity)
					.mapValues(_.size)
					.toSeq
				Page(page, aliases)
			}
	}

	def probabilityLinkDirectsToPage(alias: Alias, pageName: String): Double = {
		val totalReferences = alias
			.pages
			.toMap
			.foldLeft(0)(_ + _._2)
		val references = alias.pages.toMap.getOrElse(pageName, 0)
		references / totalReferences
	}

	def fillAliasToPagesTable(parsedWikipedia: RDD[ParsedWikipediaEntry], sc: SparkContext): Unit = {
		removeDeadLinks(groupByAliases(parsedWikipedia), getAllPages(sc))
			.saveToCassandra(keyspace, outputAliasToPagesTablename)
	}

	def fillPageToAliasesTable(parsedWikipedia: RDD[ParsedWikipediaEntry], sc: SparkContext): Unit = {
		removeDeadPages(groupByPageNames(parsedWikipedia), getAllPages(sc))
			.saveToCassandra(keyspace, outputPageToAliasesTablename)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Link Analysis")
			.set("spark.cassandra.connection.host", "odin01")
		val sc = new SparkContext(conf)

		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		fillPageToAliasesTable(parsedWikipedia, sc)
		fillAliasToPagesTable(parsedWikipedia, sc)
		sc.stop
	}
}
