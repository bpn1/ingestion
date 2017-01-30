import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import WikipediaTextparser.ParsedWikipediaEntry

object WikipediaLinkAnalysis {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputLinksTablename = "wikipedialinks"
	val outputPagesTablename = "wikipediapages"

	case class Link(alias: String, pages: Map[String, Int])

	def groupPageNamesByAliases(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Link] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.alias, link.page))
			.groupByKey
			.map { case (alias, pageList) =>
				Link(alias, pageList.groupBy(identity).mapValues(_.size))
			}
	}

	def groupAliasesByPageNames(parsedWikipedia: RDD[ParsedWikipediaEntry]): RDD[Link] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.page, link.alias))
			.groupByKey
			.map { case (page, aliasList) =>
				Link(page, aliasList.groupBy(identity).mapValues(_.size))
			}
	}

	def probabilityLinkDirectsToPage(link: String, pageName: String): Double = {
		val dummy = -1.0
		dummy
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
