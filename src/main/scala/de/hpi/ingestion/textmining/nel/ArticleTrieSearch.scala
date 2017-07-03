package de.hpi.ingestion.textmining.nel

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.AliasTrieSearch.{deserializeTrie, trieStreamFunction}
import de.hpi.ingestion.textmining.models.{TrieAlias, TrieAliasArticle, TrieNode}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Finds Trie Aliases in NEL Articles with the Trie and writes them to the same table.
  */
object ArticleTrieSearch extends SparkJob {
	appName = "Article Trie Search"
	configFile = "textmining.xml"
	sparkOptions("spark.kryo.registrator") = "de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val parsedWikipedia = sc.cassandraTable[TrieAliasArticle](settings("keyspace"), settings("NELTable"))
		List(parsedWikipedia).toAnyRDD()
	}

	/**
	  * Saves Parsed Wikipedia entries with the found aliases to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[(String, List[TrieAlias])]()
			.head
			.saveToCassandra(settings("keyspace"), settings("NELTable"), SomeColumns("id", "triealiases"))
	}
	// $COVERAGE-ON$

	/**
	  * Finds Aliases in a given Article by applying the Trie to find occurrences of known token lists.
	  *
	  * @param article   Trie Alias Article to find the aliases in
	  * @param tokenizer tokenizer used to tokenize the text
	  * @param trie      Trie used to find aliases
	  * @return Trie Alias Article enriched with the found aliases
	  */
	def findAliases(article: TrieAliasArticle, tokenizer: IngestionTokenizer, trie: TrieNode): TrieAliasArticle = {
		val text = article.text.getOrElse("")
		val tokens = tokenizer.processWithOffsets(text)
		var invalidIndices = Set.empty[Int]
		val foundAliases = tokens.indices
			.flatMap { i =>
				val testTokens = tokens.slice(i, tokens.length)
				val aliasMatches = trie.matchTokens(testTokens.map(_.token))
				val offsetMatches = aliasMatches.map(textTokens => testTokens.take(textTokens.length))
				offsetMatches
					.sortBy(_.length)
					.lastOption
					.flatMap { longestMatch =>
						invalidIndices ++= longestMatch.indices.slice(1, longestMatch.length).map(_ + i)
						val offset = longestMatch.headOption.map(_.beginOffset)
						offset.map { begin =>
							val alias = text.substring(begin, longestMatch.last.endOffset)
							(i, TrieAlias(alias, offset))
						}
					}
			}.collect {
			case (index: Int, alias: TrieAlias) if !invalidIndices.contains(index) => alias
		}.toList
		article.copy(triealiases = foundAliases)
	}

	/**
	  * Finds Aliases in the Articles using the Trie.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[TrieAliasArticle]().head
		val tokenizer = IngestionTokenizer(false, false)
		val aliasArticles = articles
			.mapPartitions({ partition =>
				val trie = deserializeTrie(trieStreamFunction(settings("smallerTrieFile")))
				partition.map(findAliases(_, tokenizer, trie))
			}, true)
		val trieAliases = aliasArticles.map(article => (article.id, article.triealiases))
		List(trieAliases).toAnyRDD()
	}
}
