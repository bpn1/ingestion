package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.jsoup.Jsoup
import play.api.libs.json.JsValue
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.textmining.AliasTrieSearch.{deserializeTrie, trieStreamFunction}
import de.hpi.ingestion.textmining.models.{TrieAlias, TrieNode}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

/**
  * Parses the Spiegel JSON dump to Spiegel Articles, parses the HTML to raw text and saves them to the Cassandra.
  */
object SpiegelImport extends SparkJob with JSONParser[SpiegelArticle] {
	appName = "Spiegel Import"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the Spiegel JSON dump from the HDFS.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val spiegel = sc.textFile(settings("spiegelFile"))
		List(spiegel).toAnyRDD()
	}

	/**
	  * Saves the parsed Spiegel Articles to the cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[SpiegelArticle]()
			.head
			.saveToCassandra(settings("keyspace"), settings("spiegelTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Parses the Spiegel JSON dump into Spiegel Articles.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val spiegelDump = input.fromAnyRDD().head
		val tokenizer = IngestionTokenizer(false, false)
		val parsedArticles = spiegelDump
			.map(parseJSON)
			.mapPartitions({ partition =>
				val trie = deserializeTrie(trieStreamFunction(settings("smallerTrieFile")))
				partition.map(findAliases(_, tokenizer, trie))
			}, true)
		List(parsedArticles).toAnyRDD()
	}

	/**
	  * Finds Aliases in a given Spiegel Article by applying the given Trie to find occurrences of known token lists.
	  * @param article Spiegel Article to find the aliases in
	  * @param tokenizer tokenizer used to tokenize the text
	  * @param trie Trie used to find aliases
	  * @return Spiegel Article enriched with the found aliases
	  */
	def findAliases(article: SpiegelArticle, tokenizer: IngestionTokenizer, trie: TrieNode): SpiegelArticle = {
		val text = article.text.getOrElse("")
		val tokens = tokenizer.processWithOffsets(text)
		val foundAliases = tokens.indices
			.flatMap { i =>
				val testTokens = tokens.slice(i, tokens.length)
				val aliasMatches = trie.matchTokens(testTokens.map(_.token))
				val offsetMatches = aliasMatches.map(textTokens => testTokens.take(textTokens.length))
				offsetMatches
					.sortBy(_.length)
					.lastOption
					.flatMap { longestMatch =>
						val offset = longestMatch.headOption.map(_.beginOffset)
						offset.map { begin =>
							val alias = text.substring(begin, longestMatch.last.endOffset)
							TrieAlias(alias, offset)
						}
				}
			}.toList
		article.copy(triealiases = foundAliases)
	}

	/**
	  * Extracts the article data from a given JSON object, parses the HTML content into text and finds the occurring
	  * aliases in the text.
	  * @param json JSON object containing the article data
	  * @return Spiegel Article containing the parsed JSON data
	  */
	override def fillEntityValues(json: JsValue): SpiegelArticle = {
		val id = extractString(json, List("_id", "$oid")).get
		val url = extractString(json, List("url"))
		val title = extractString(json, List("title"))
		val content = extractString(json, List("content"))
		val text = content
			.map(_.replaceAll("&nbsp;", " "))
			.map(Jsoup.parse)
			.map(htmlDoc => s"${htmlDoc.head().text()} ${htmlDoc.body().text()}".trim)
		SpiegelArticle(id, title, text, url)
	}
}
