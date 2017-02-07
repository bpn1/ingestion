import WikipediaTextparser.ParsedWikipediaEntry
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaAliasCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val inputAliasesTablename = "wikipedialinks"
	val outputTablename = "wikipediaaliases"

	case class AliasCounter(alias: String, var linkOccurrences: Int = 0, var totalOccurrences: Int = 0)

	case class AliasOccurrencesInArticle(links: scala.collection.mutable.Set[String], noLinks: scala.collection.mutable.Set[String])

	def identifyAliasOccurrencesInArticle(article: ParsedWikipediaEntry, allAliases: List[String]): AliasOccurrencesInArticle = {
		val links = scala.collection.mutable.Set[String]()
		val noLinks = scala.collection.mutable.Set[String]()
		allAliases
			.foreach { alias =>
				if (article.links.exists(link => link.alias == alias))
					links += alias
				else if (article.text.get contains alias)
					noLinks += alias
			}
		AliasOccurrencesInArticle(links, noLinks)
	}

	def countAllAliasOccurrences(articles: RDD[ParsedWikipediaEntry], aliases: RDD[String], sc: SparkContext): RDD[AliasCounter] = {
		val aliasesList = aliases
			.collect
			.toList

		articles
			.map(article => identifyAliasOccurrencesInArticle(article, aliasesList))
			.flatMap { occurrences =>
				val links = occurrences.links
					.map(alias => (alias, true))
				val noLinks = occurrences.noLinks
					.map(alias => (alias, false))
				links ++ noLinks
			}
			.groupBy(identity)
			.map { case ((alias, isLink), occurrences) => (alias, isLink, occurrences.size) }
			.groupBy { case (alias, isLink, count) => alias }
			.map { case ((alias, counters)) =>
				// at most one counter for links and one counter for no link
				assert(counters.size <= 2)

				var linkOccurrences = 0
				var totalOccurrences = 0

				counters
					.foreach { case (alias1, isLink, count) =>
						if (isLink)
							linkOccurrences += count
						totalOccurrences += count
					}
				AliasCounter(alias, linkOccurrences, totalOccurrences)
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Alias Counter")
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val allAliases = sc.cassandraTable[WikipediaLinkAnalysis.Link](keyspace, inputAliasesTablename)
			.map(_.alias)

		countAllAliasOccurrences(allArticles, allAliases, sc)
			.saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
