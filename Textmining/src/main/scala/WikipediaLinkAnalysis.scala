import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import WikipediaTextparser.ParsedWikipediaEntry
import com.holdenkarau.spark.testing.SharedSparkContext

object WikipediaLinkAnalysis {
	val keyspace = "wikidumps"
	val rawInputTablename = "wikipedia"
	val inputTablename = "parsedwikipedia"
	val outputLinksTablename = "wikipedialinks"
	val outputPagesTablename = "wikipediapages"

	case class Link(alias: String, pages: Seq[(String, Int)])

	// Seq instead of Map according to http://stackoverflow.com/questions/17709995/notserializableexception-for-mapstring-string-alias

	def getAllPages(sc: SparkContext): RDD[String] = {
		sc.cassandraTable[WikipediaTextparser.WikipediaEntry](keyspace, rawInputTablename)
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
				val links = rawLinks.map{case(alias, pageName, count) => (pageName, count)}.toSeq
				Link(alias, links)
			}
	}

	def groupPageNamesByAliases(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Link] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.alias, link.page))
			.groupByKey
			.map { case (alias, pageList) =>
				Link(alias, pageList
					.groupBy(identity)
					.mapValues(_.size)
					.toSeq)
			}
	}

	def groupAliasesByPageNames(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Link] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.page, link.alias))
			.groupByKey
			.map { case (page, aliasList) =>
				Link(page, aliasList
					.groupBy(identity)
					.mapValues(_.size)
					.toSeq)
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

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Link Extraction")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputTablename)
		groupPageNamesByAliases(parsedWikipedia)
			.saveToCassandra(keyspace, outputLinksTablename)
		sc.stop
	}
}
