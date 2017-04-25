package de.hpi.ingestion.textmining

import java.io.InputStream

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.Kryo
import de.hpi.ingestion.textmining.models._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Finds all occurrences of the aliases in the given trie in all Wikipedia articles and writes them to the
  * foundaliases column.
  */
object AliasTrieSearch {
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"
	val trieName = "trie_full.bin"

	/**
	  * Finds all occurrences of aliases in the text of a Wikipedia entry.
	  * @param entry parsed Wikipedia entry to use
	  * @param trie Trie containing the aliases we look for
	  * @param tokenizer Tokenizer used to tokenize the text of the entry
	  * @return entry containing list of found aliases
	  */
	def matchEntry(
		entry: ParsedWikipediaEntry,
		trie: TrieNode,
		tokenizer: IngestionTokenizer
	): ParsedWikipediaEntry = {
		val resultList = mutable.ListBuffer[String]()
		val tokens = tokenizer.process(entry.getText())
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens)
			resultList ++= aliasMatches.map(tokenizer.reverse)
		}
		entry.foundaliases = cleanFoundAliases(resultList.toList)
		entry
	}

	/**
	  * Removes empty and duplicate aliases from the given list of aliases.
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
	  * Opens a HDFS file stream pointing to the trie binary.
	  * @return Input Stream pointing to the file in the HDFS
	  */
	def hdfsFileStream(): FSDataInputStream = {
		val hadoopConf = new Configuration()
		val fs = FileSystem.get(hadoopConf)
		fs.open(new Path(trieName))
	}

	/**
	  * Uses a pre-built Trie to find aliases in the text of articles and writes them into the foundaliases field.
	  * @param articles RDD of Parsed Wikipedia Articles
	  * @return RDD of Parsed Wikipedia Articles containing the aliases found in their text
	  */
	def run(articles: RDD[ParsedWikipediaEntry]): RDD[ParsedWikipediaEntry] = {
		val tokenizer = IngestionTokenizer(false, false)
		articles
			.mapPartitions({ partition =>
				val trie = deserializeTrie(hdfsFileStream())
				partition.map(matchEntry(_, trie, tokenizer))
			}, true)
	}

	// recommended spark-submit flags
	// --driver-memory 16G				<< needed for building the Trie
	// --driver-java-options -Xss1g		<< needed for serialization of Trie
	// --conf spark.executor.extrajavaoptions="-Xss8g"	<< not sure if needed
	// --conf spark.yarn.am.extraJavaOptions="-Xss8g"	<< not sure if needed
	// --conf "spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000"
	// ThreadStackSize is needed for deserialization of Trie
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("TrieBuilder")
		 	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.kryo.registrator", "de.hpi.ingestion.textmining.TrieKryoRegistrator")
		val sc = new SparkContext(conf)
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
		run(parsedWikipedia).saveToCassandra(keyspace, tablename)
		sc.stop
	}
}
