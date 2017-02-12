import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import WikiClasses._

object WikipediaAliasCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputTablename = "wikipediaaliases"

	def identifyAliasOccurrencesInArticle(article: ParsedWikipediaEntry): AliasOccurrencesInArticle = {
		val links = article.links
			.map(_.alias)
			.toSet
		val noLinks = article.foundaliases.toSet
			.filterNot(links)
		AliasOccurrencesInArticle(links, noLinks)
	}

	def countAllAliasOccurrences(articles: RDD[ParsedWikipediaEntry]): RDD[AliasCounter] = {
		val allAliasOccurrences = articles
			.map(article => identifyAliasOccurrencesInArticle(article))
			.flatMap { occurrences =>
				val links = occurrences.links
					.map(alias => (alias, true))
				val noLinks = occurrences.noLinks
					.map(alias => (alias, false))
				links ++ noLinks
			}

		allAliasOccurrences
			.map(_._1)
			.filter(alias => alias.length > 100)
			.collect
			.foreach(alias => println("[WikipediaAliasCounter ERROR]    Very long alias: " + alias.take(100)))

		allAliasOccurrences
			.filter { case (alias, isLink) => alias.nonEmpty && alias.length < 100 }
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

	def probabilityIsLink(aliasCounter: WikiClasses.AliasCounter): Double = {
		aliasCounter.linkoccurrences.toDouble / aliasCounter.totaloccurrences
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Alias Counter")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		countAllAliasOccurrences(allArticles)
			.saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
