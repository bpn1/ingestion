import WikiClasses.{LinkContext, ParsedWikipediaEntry, DocumentFrequency}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import scala.collection.mutable

object WikipediaContextExtractor {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputDocumentFrequenciesTablename = "wikipediadocfreq"
	val outputContextsTablename = "wikipediacontexts"

	def countDocumentFrequencies(articles: RDD[ParsedWikipediaEntry]): RDD[DocumentFrequency] = {
		val tokenizer = new CleanCoreNLPTokenizer
		articles
			.flatMap(article => textToWordSet(article.text.get, tokenizer))
			.map(word => (word, 1))
			.reduceByKey(_ + _)
			.map { case (word, count) => DocumentFrequency(word, count) }
	}

	def textToWordSet(text: String, tokenizer: Tokenizer): Set[String] = {
		tokenizer.tokenize(text).toSet
	}

	def extractLinkContextsFromArticle(article: ParsedWikipediaEntry, tokenizer: Tokenizer): Set[LinkContext] = {
		// Do not initialize tokenizer here so it is initialized only once.
		// (probably important for good performance of CoreNLP)
		val wordSet = textToWordSet(article.getText, tokenizer)
		article.links
			.map(link => LinkContext(link.page, wordSet))
			.toSet
	}

	def extractAllContexts(articles: RDD[ParsedWikipediaEntry]): RDD[LinkContext] = {
		val tokenizer = new CleanCoreNLPTokenizer
		articles
			.flatMap(article => extractLinkContextsFromArticle(article, tokenizer))
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
			.setAppName("Wikipedia Context Extractor")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		countDocumentFrequencies(allArticles)
			.saveToCassandra(keyspace, outputDocumentFrequenciesTablename)
		//		extractAllContexts(allArticles)
		//			.saveToCassandra(keyspace, outputContextsTablename)

		sc.stop()
	}
}