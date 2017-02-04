import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import WikipediaTextparser.ParsedWikipediaEntry

object WikipediaLinkAnalysis {
	val keyspace = "wikidumps"
	val inputRawTablename = "wikipedia"
	val inputParsedTablename = "parsedwikipedia"
	val outputAliasToPagesTablename = "wikipedialinks"
	val outputPageToAliasesTablename = "wikipediapages"

	case class Link(alias: String, pages: Seq[(String, Int)])

	case class Page(page: String, aliases: Seq[(String, Int)])

	// Seq instead of Map according to http://stackoverflow.com/questions/17709995/notserializableexception-for-mapstring-string-alias

	def getAllPages(sc: SparkContext): RDD[String] = {
		sc.cassandraTable[WikipediaTextparser.WikipediaEntry](keyspace, inputRawTablename)
			.map(_.title)
	}

	def removeDeadLinks(links: RDD[Link], allPages: RDD[String]): RDD[Link] = {
		links
			.flatMap(link => link.pages.map(page => (link.alias, page._1, page._2)))
			.keyBy { case (alias, pageName, count) => pageName }
			.join(allPages.map(entry => (entry, entry)).keyBy(_._1)) // kill somebody for this
			.map { case (pageName, (link, doublePageName)) => link }
			.groupBy { case (alias, pageName, count) => alias }
			.map { case (alias, rawLinks) =>
				val links = rawLinks.map { case (alias, pageName, count) => (pageName, count) }.toSeq
				Link(alias, links)
			}
	}

	def groupByAliases(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Link] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.alias, link.page))
			.groupByKey
			.map { case (alias, pageList) =>
				val pages = pageList
					.groupBy(identity)
					.mapValues(_.size)
					.toSeq
				Link(alias, pages)
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

	def probabilityLinkDirectsToPage(link: Link, pageName: String): Double = {
		val totalReferences = link
			.pages
			.toMap
			.foldLeft(0)(_ + _._2)
		val references = link.pages.toMap.getOrElse(pageName, 0)
		references / totalReferences
	}

	def fillAliasToPagesTable(parsedWikipedia: RDD[ParsedWikipediaEntry], sc: SparkContext): Unit = {
		removeDeadLinks(groupByAliases(parsedWikipedia), getAllPages(sc))
			.saveToCassandra(keyspace, outputAliasToPagesTablename)
	}

	def fillPageToAliasesTable(parsedWikipedia: RDD[ParsedWikipediaEntry]): Unit = {
		groupByPageNames(parsedWikipedia)
			.saveToCassandra(keyspace, outputPageToAliasesTablename)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Link Extraction")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputParsedTablename)
		fillAliasToPagesTable(parsedWikipedia, sc)
		fillPageToAliasesTable(parsedWikipedia)
		sc.stop
	}
}
