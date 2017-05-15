package de.hpi.ingestion.textmining

import java.io.InputStream

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.Kryo
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

import scala.collection.mutable.ListBuffer

/**
  * Finds all occurrences of the aliases in the given trie in all Wikipedia articles and writes them to the
  * foundaliases column.
  * recommended spark-submit flags
  * --driver-memory 16G				<< needed for building the Trie
  * --driver-java-options -Xss1g	<< needed for serialization of Trie
  * --conf "spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000" << needed for deserialization of Trie
  */
object AliasTrieSearch extends SparkJob {
	appName = "Alias Trie Search"
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"
	val trieName = "trie_full.bin"
	sparkOptions("spark.kryo.registrator") = "de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator"

	// $COVERAGE-OFF$
	var trieStreamFunction = hdfsFileStream _
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
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
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.saveToCassandra(keyspace, tablename)
	}

	/**
	  * Opens a HDFS file stream pointing to the trie binary.
	  *
	  * @return Input Stream pointing to the file in the HDFS
	  */
	def hdfsFileStream(): InputStream = {
		val hadoopConf = new Configuration()
		val fs = FileSystem.get(hadoopConf)
		fs.open(new Path(trieName))
	}
	// $COVERAGE-ON$

	/**
	  * Finds all occurrences of aliases in the text of a Wikipedia entry. All found aliases are saved in the
	  * foundalias column and the longest match of each alias is saved with its offset and context in the triealiases
	  * column. These triealiases are only those which are not an extended link.
	  *
	  * @param entry     parsed Wikipedia entry to use
	  * @param trie      Trie containing the aliases we look for
	  * @param tokenizer Tokenizer used to tokenize the text of the entry
	  * @return entry containing list of found aliases
	  */
	def matchEntry(
		entry: ParsedWikipediaEntry,
		trie: TrieNode,
		tokenizer: IngestionTokenizer
	): ParsedWikipediaEntry = {
		val resultList = ListBuffer[String]()
		var contextAliases = List[TrieAlias]()
		val tokens = tokenizer.processWithOffsets(entry.getText())
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens.map(_.token))
			val offsetMatches = aliasMatches.map(tokens => testTokens.take(tokens.length))
			resultList ++= offsetMatches.map(tokens =>
				entry.getText().substring(tokens.head.beginOffset, tokens.last.endOffset))
			if(aliasMatches.nonEmpty) {
				val longestMatch = offsetMatches.maxBy(_.length)
				val offset = Option(longestMatch.head.beginOffset)
				val alias = entry.getText().substring(longestMatch.head.beginOffset, longestMatch.last.endOffset)
				val context = extractAliasContext(longestMatch, tokens, i).getCounts()
				contextAliases :+= TrieAlias(alias, offset, context)
			}
		}

		contextAliases = contextAliases.filterNot(alias =>
			entry.extendedlinks.exists(link => link.offset == alias.offset && link.alias == alias.alias))
		entry.copy(foundaliases = cleanFoundAliases(resultList.toList), triealiases = contextAliases)
	}

	/**
	  * Extracts the context of a found alias from the tokenized text and returns a Bag of words of this context.
	  * @param alias tokenized alias
	  * @param text tokenized text
	  * @param index offset of the tokenized alias in the tokenized text
	  * @return Bag of words of the context of the alias
	  */
	def extractAliasContext(alias: List[OffsetToken], text: List[OffsetToken], index: Int): Bag[String, Int] = {
		val tokenizer = IngestionTokenizer(true, true)
		val contextSize = TermFrequencyCounter.contextSize
		val contextStart = Math.max(index - contextSize, 0)
		val preAliasContext = text.slice(contextStart, index)
		val aliasEndIndex = index + alias.length
		val contextEnd = Math.min(aliasEndIndex + contextSize, text.length)
		val postAliasContext = text.slice(aliasEndIndex, contextEnd)
		val context = (preAliasContext ++ postAliasContext).map(_.token)
		TermFrequencyCounter.extractBagOfWords(tokenizer.process(context))
	}

	/**
	  * Removes empty and duplicate aliases from the given list of aliases.
	  *
	  * @param aliases List of aliases found in an article
	  * @return cleaned List of aliases
	  */
	def cleanFoundAliases(aliases: List[String]): List[String] = {
		aliases
			.filter(_.nonEmpty)
			.distinct
	}

	/**
	  * Deserializes Trie binary into a TrieNode.
	  *
	  * @param trieStream Input Stream pointing to the Trie binary data
	  * @return deserialized Trie
	  */
	def deserializeTrie(trieStream: InputStream): TrieNode = {
		val kryo = new Kryo()
		TrieKryoRegistrator.register(kryo)
		val inputStream = new Input(trieStream)
		val trie = kryo.readObject(inputStream, classOf[TrieNode])
		inputStream.close()
		trie
	}

	/**
	  * Uses a pre-built Trie to find aliases in the text of articles and writes them into the foundaliases field.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val tokenizer = IngestionTokenizer(false, false)
		input
			.fromAnyRDD[ParsedWikipediaEntry]()
			.map(articles =>
				articles
					.mapPartitions({ partition =>
						val trie = deserializeTrie(trieStreamFunction())
						partition.map(matchEntry(_, trie, tokenizer))
					}, true))
			.toAnyRDD()
	}
}
