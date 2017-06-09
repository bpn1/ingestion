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
  * recommended spark-submit flags (needed for deserialization of Trie)
  * --conf "spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000"
  */
object AliasTrieSearch extends SparkJob {
	appName = "Alias Trie Search"
	configFile = "textmining.xml"
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
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
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
			.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
	}

	/**
	  * Opens a HDFS file stream pointing to the trie binary.
	  *
	  * @return Input Stream pointing to the file in the HDFS
	  */
	def hdfsFileStream(file: String = settings("trieFile")): InputStream = {
		val hadoopConf = new Configuration()
		val fs = FileSystem.get(hadoopConf)
		fs.open(new Path(file))
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
		tokenizer: IngestionTokenizer,
		contextTokenizer: IngestionTokenizer,
		settings: Map[String, String] = this.settings
	): ParsedWikipediaEntry = {
		val resultList = ListBuffer[List[OffsetToken]]()
		var contextAliases = List[TrieAlias]()
		val tokens = tokenizer.processWithOffsets(entry.getText())
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens.map(_.token))
			val offsetMatches = aliasMatches.map(textTokens => testTokens.take(textTokens.length))
			resultList ++= offsetMatches
			if(aliasMatches.nonEmpty) {
				val longestMatch = offsetMatches.maxBy(_.length)
				val offset = longestMatch.headOption.map(_.beginOffset)
				offset.foreach { begin =>
					val alias = entry.getText().substring(begin, longestMatch.last.endOffset)
					contextAliases :+= TrieAlias(alias, offset)
				}
			}
		}

		contextAliases = contextAliases.filterNot { alias =>
			val isLink = entry.textlinks.exists(link => link.offset == alias.offset && link.alias == alias.alias)
			val isExLink = entry.extendedlinks().exists(_.offset == alias.offset)
			isLink || isExLink
		}

		val foundAliases = resultList
			.filter(_.nonEmpty)
			.map { offsetTokens =>
				val begin = offsetTokens.headOption.map(_.beginOffset)
				val end = offsetTokens.lastOption.map(_.endOffset)
				(begin, end)
			}.collect {
				case (begin, end) if begin.isDefined && end.isDefined =>
					entry.getText().substring(begin.get, end.get)
			}.toList

		entry.copy(foundaliases = cleanFoundAliases(foundAliases), triealiases = contextAliases)
	}

	/**
	  * Removes empty aliases from the given list of aliases.
	  *
	  * @param aliases List of aliases found in an article
	  * @return cleaned List of aliases
	  */
	def cleanFoundAliases(aliases: List[String]): List[String] = {
		aliases.filter(_.nonEmpty)
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
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val tokenizer = IngestionTokenizer(false, false)
		val contextTokenizer = IngestionTokenizer(true, true)
		val settingsBroadcast = sc.broadcast(settings)
		input
			.fromAnyRDD[ParsedWikipediaEntry]()
			.map(articles =>
				articles
					.mapPartitions({ partition =>
						val localSettings = settingsBroadcast.value
						val trie = deserializeTrie(trieStreamFunction(localSettings("trieFile")))
						partition.map(matchEntry(_, trie, tokenizer, contextTokenizer, localSettings))
					}, true))
			.toAnyRDD()
	}
}
