import WikiClasses.{LinkContext, ParsedWikipediaEntry, DocumentFrequency}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaContextExtractor {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputDocumentFrequenciesTablename = "wikipediadocfreq"
	val outputContextsTablename = "wikipediacontexts"
	val removeStopwords = true
	val stopwordsPath = "german_stopwords.txt"

	def countDocumentFrequencies(
		articles: RDD[ParsedWikipediaEntry],
		stopwords: Set[String] = Set[String]()
	): RDD[DocumentFrequency] =
	{
		val tokenizer = new CleanCoreNLPTokenizer
		articles
			.flatMap(article => textToWordSet(article.getText, tokenizer))
			.map(word => (word, 1))
			.reduceByKey(_ + _)
			.map { case (word, count) => DocumentFrequency(word, count) }
			.filter(documentFrequency => !stopwords.contains(documentFrequency.word))
	}

	def textToWordSet(text: String, tokenizer: Tokenizer): Set[String] = {
		tokenizer.tokenize(text).toSet
	}

	def extractLinkContextsFromArticle(
		article: ParsedWikipediaEntry,
		tokenizer: Tokenizer): Set[LinkContext] = {
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
			.map(linkContext => (linkContext.pagename, linkContext.words))
			.reduceByKey(_ ++ _)
			.map { case (pagename, contextSum) => LinkContext(pagename, contextSum) }
	}

	def loadStopwords(sc: SparkContext): Set[String] = {
		var stopwords = Set[String]()
		if (removeStopwords) {
			stopwords = sc
				.textFile(stopwordsPath)
				.collect
				.toSet
		}
		stopwords
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Context Extractor")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val stopwords = loadStopwords(sc)
		countDocumentFrequencies(allArticles, stopwords)
			.saveToCassandra(keyspace, outputDocumentFrequenciesTablename)
		//		extractAllContexts(allArticles)
		//			.saveToCassandra(keyspace, outputContextsTablename)

		sc.stop()
	}
}
