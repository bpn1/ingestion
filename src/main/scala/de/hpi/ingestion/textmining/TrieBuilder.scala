package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

import scala.collection.mutable
import de.hpi.ingestion.textmining.models._

object TrieBuilder {
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"

	/**
	  * Finds all occurences of aliases in the text of a Wikipedia entry
	  * @param entry parsed Wikipedia entry to use
	  * @param trie Trie containing the aliases we look for
	  * @param tokenizer Tokenizer used to tokenize the text of the entry
	  * @return entry containing list of found aliases
	  */
	def matchEntry(
		entry: ParsedWikipediaEntry,
		trie: TrieNode,
		tokenizer: Tokenizer
	): ParsedWikipediaEntry =
	{
		val resultList = mutable.ListBuffer[String]()
		val tokens = tokenizer.tokenize(entry.getText())
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens)
			resultList ++= aliasMatches.map(tokenizer.reverse)
		}
		entry.foundaliases = resultList
			.filter(_ != "")
			.distinct
			.toList
		entry
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
			.set("spark.cassandra.connection.host", "odin01")
		 	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.set("spark.kryo.registrator", "TrieKryoRegistrator")
		val sc = new SparkContext(conf)
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
		val aliasList = parsedWikipedia
			.flatMap(_.allLinks)
			.map(_.alias)
			.distinct
			.collect

		val tokenizer = new CleanCoreNLPTokenizer()
		val localTrie = new TrieNode()
		for(alias <- aliasList) {
			localTrie.append(tokenizer.tokenize(alias))
		}
		val trieBroadcast = sc.broadcast(localTrie)

		parsedWikipedia
			.mapPartitions({ partition =>
				val trie = trieBroadcast.value
				partition.map(entry => matchEntry(entry, trie, tokenizer))
			}, true)
			.saveToCassandra(keyspace, tablename)

		sc.stop
	}
}
