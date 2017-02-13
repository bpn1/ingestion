import WikiClasses.{LinkContext, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import scala.collection.mutable

object WikipediaContextExtractor {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputTablename = "wikipediacontexts"

	def extractLinkContextsFromArticle(article: ParsedWikipediaEntry): List[LinkContext] = {
		article.links
			.map(link => LinkContext(link.page, Set[String]("lorem ipsum")))
	}

	def extractAllContexts(articles: RDD[ParsedWikipediaEntry]): RDD[LinkContext] = {
		articles
			.map(extractLinkContextsFromArticle)
			.flatMap(contexts => contexts)
			.groupBy(_.pagename)
			.map { case (pagename, contexts) =>
				val contextSum = contexts
					.map(context => mutable.Set(context.words.toSeq: _*))
					.reduceLeft((a, b) => a.union(b))
				    .toSet
				LinkContext(pagename, contextSum)
			}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Alias Counter")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		extractAllContexts(allArticles)
			.saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
