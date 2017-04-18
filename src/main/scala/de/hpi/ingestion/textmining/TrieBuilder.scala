package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.esotericsoftware.kryo.io.Input

import scala.collection.mutable
import de.hpi.ingestion.textmining.models._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.serializer.KryoSerializer

object TrieBuilder {
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"
	val defaultKey = "default"

	def keyList(): List[String] = {
		val upperCaseLetters = (65 to 90).map(_.toChar.toString).toList
		val lowerCaseLetters = (97 to 122).map(_.toChar.toString).toList
		val defaultKey = List("default")
		upperCaseLetters ++ lowerCaseLetters ++ defaultKey
	}

	def fileName(key: String): String = {
		s"trie_binaries/trie_$key.bin"
	}

	/**
	  * Finds all occurences of aliases in the text of a Wikipedia entry
	  * @param entry parsed Wikipedia entry to use
	  * @param trieMap Map of the Tries containing the aliases we look for
	  * @param tokenizer Tokenizer used to tokenize the text of the entry
	  * @return entry containing list of found aliases
	  */
	def matchEntry(
		entry: ParsedWikipediaEntry,
		trieMap: Map[String, TrieNode],
		tokenizer: Tokenizer
	): ParsedWikipediaEntry = {
		val resultList = mutable.ListBuffer[String]()
		val tokens = tokenizer.tokenize(entry.getText())
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val firstChar = testTokens.headOption
				.map(_.charAt(0))
			if(firstChar.isDefined) {
				val char = firstChar.get
				val trie = if(char.isLetter && char <= 'z') {
					trieMap(char.toString)
				} else {
					trieMap(defaultKey)
				}
				val aliasMatches = trie.matchTokens(testTokens)
				resultList ++= aliasMatches.map(tokenizer.reverse)
			}
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
			.set("spark.kryo.registrator", "de.hpi.ingestion.textmining.TrieKryoRegistrator")
		val sc = new SparkContext(conf)
		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
//		val aliasList = parsedWikipedia
//			.flatMap(_.allLinks)
//			.map(_.alias)
//			.distinct
//			.collect
//
//
//		val localTrie = new TrieNode()
//		for(alias <- aliasList) {
//			localTrie.append(tokenizer.tokenize(alias))
//		}
//		val trieBroadcast = sc.broadcast(localTrie)

		val fs = FileSystem.get(sc.hadoopConfiguration)

		val trieMap = keyList()
		    .map(key => (key, fileName(key)))
			.toMap
		    .mapValues(new Path(_))
		    .mapValues(fs.open)
			.map(identity)

		val trieBroadcast = sc.broadcast(trieMap)
		val tokenizer = new CleanCoreNLPTokenizer
		val kryo = new KryoSerializer(conf).newKryo()

		parsedWikipedia
			.mapPartitions({ partition =>
				val localTrieMap = trieBroadcast.value
					.mapValues { file =>
						val inputStream = new Input(file)
						val trie = kryo.readObject(inputStream, classOf[TrieNode])
						inputStream.close()
						trie
					}.map(identity)
				partition.map(entry => matchEntry(entry, localTrieMap, tokenizer))
			}, true)
			.saveToCassandra(keyspace, tablename)

		sc.stop
	}
}
