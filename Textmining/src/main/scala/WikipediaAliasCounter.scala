import WikipediaTextparser.ParsedWikipediaEntry
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

import scala.collection.mutable.ListBuffer

object WikipediaAliasCounter {
	val keyspace = "wikidumps"
	val inputTablename = "wikipedialinks"
	val outputTablename = "wikipediaaliases"

	case class AliasCounter(alias: String, var linkOccurences: Int = 0, var totalOccurences: Int = 0)

	case class AliasOccurrencesInArticle(links: List[String], noLinks: List[String])

	def identifyAliasOccurrencesInArticle(article: ParsedWikipediaEntry, allAliases: List[String]): AliasOccurrencesInArticle = {
		val links = ListBuffer[String]()
		val noLinks = ListBuffer[String]()
		allAliases
			.foreach { alias =>
				if (article.links.exists(link => link.alias == alias))
					links += alias
				else if (article.text.get contains alias)
					noLinks += alias
			}
		AliasOccurrencesInArticle(links.toList, noLinks.toList)
	}

	def countAliasOccurrences(alias: String): AliasCounter = {
		AliasCounter(alias)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Alias Counter")
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		sc.cassandraTable[WikipediaLinkAnalysis.Link](keyspace, inputTablename)
			.map(link => countAliasOccurrences(link.alias))
			.saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
