import WikiClasses.{DocumentFrequency, LinkContext, ParsedWikipediaEntry}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

import scala.io.Source

object WikipediaContextExtractor {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val outputDocumentFrequenciesTablename = "wikipediadocfreq"
	val outputContextsTablename = "wikipediacontexts"
	val removeStopwords = true
	val stopwordsPath = "german_stopwords.txt"
	val leastSignificantDocumentFrequency = 5

	def stem(word: String): String = {
		val stemmer = new AccessibleGermanStemmer
		stemmer.stem(word)
	}

	def stemDocumentFrequencies(
		documentFrequencies: RDD[DocumentFrequency]
	): RDD[DocumentFrequency] = {
		documentFrequencies
			.map(df => (stem(df.word), df.count))
			.reduceByKey(_ + _)
			.map { case (word, count) => DocumentFrequency(word, count) }
	}

	def countDocumentFrequencies(
		articles: RDD[ParsedWikipediaEntry]
	): RDD[DocumentFrequency] = {
		val tokenizer = new CleanCoreNLPTokenizer
		articles
			.flatMap(article => textToWordSet(article.getText(), tokenizer))
			.map(word => (word, 1))
			.reduceByKey(_ + _)
			.map { case (word, count) => DocumentFrequency(word, count) }
	}

	def filterDocumentFrequencies(
		documentFrequencies: RDD[DocumentFrequency],
		threshold: Int,
		stopwords: Set[String]
	): RDD[DocumentFrequency] = {
		documentFrequencies
			.filter(documentFrequency => !stopwords.contains(documentFrequency.word) &&
				documentFrequency.count >= threshold)
	}

	def textToWordSet(text: String, tokenizer: Tokenizer): Set[String] = {
		tokenizer.tokenize(text).toSet
	}

	def extractLinkContextsFromArticle(
		article: ParsedWikipediaEntry,
		tokenizer: Tokenizer): Set[LinkContext] = {
		// Do not initialize tokenizer here so it is initialized only once.
		// (probably important for good performance of CoreNLP)
		val wordSet = textToWordSet(article.getText(), tokenizer)
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

	def loadStopwords(): Set[String] = {
		var stopwords = Set[String]()
		if (removeStopwords) {
			stopwords = Source.fromURL(getClass.getResource(s"/$stopwordsPath"))
				.getLines()
				.toSet
		}
		stopwords
	}

	def getDocumentFrequency(
		word: String,
		documentFrequencyTable: RDD[DocumentFrequency],
		frequencyThreshold: Int
	): Int = {
		val documentFrequency = documentFrequencyTable
			.filter(_.word == word)
			.collect
			.toList

		assert(documentFrequency.length <= 1) // otherwise word is not a primary key
		if (documentFrequency.nonEmpty) {
			documentFrequency.head.count
		}
		else {
			frequencyThreshold - 1
		}
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Context Extractor")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)
		val allArticles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val stopwords = loadStopwords()
		val documentFrequencies = countDocumentFrequencies(allArticles)
		val filteredDocumentFrequencies = filterDocumentFrequencies(
			documentFrequencies,
			leastSignificantDocumentFrequency,
			stopwords
		)
		val stemmedDocuementFrequencies = stemDocumentFrequencies(filteredDocumentFrequencies)
		stemmedDocuementFrequencies
			.saveToCassandra(keyspace, outputDocumentFrequenciesTablename)
		//		extractAllContexts(allArticles)
		//			.saveToCassandra(keyspace, outputContextsTablename)

		sc.stop()
	}
}
