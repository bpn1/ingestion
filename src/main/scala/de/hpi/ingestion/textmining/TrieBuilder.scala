package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import com.datastax.spark.connector._
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.Kryo
import de.hpi.ingestion.implicits.CharImplicits._
import de.hpi.ingestion.textmining.models._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable

object TrieBuilder {
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"
	val path = "trie_binaries/"
	val defaultKey = "default"

	/**
	  * Finds all occurences of aliases in the text of a Wikipedia entry
	  * @param entry parsed Wikipedia entry to use
	  * @param trieMap Partitioned Trie containing the aliases we look for
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
			val key = tokens.headOption
				.filter(_.nonEmpty)
				.map(_.charAt(0))
				.filter(_.isASCIILetter)
				.map(_.toString)
				.getOrElse(defaultKey)
			val aliasMatches = trieMap(key).matchTokens(testTokens)
			resultList ++= aliasMatches.map(tokenizer.reverse)
		}
		entry.foundaliases = resultList
			.filter(_ != "")
			.distinct
			.toList
		entry
	}

	def initializeTrie(sc: SparkContext): TrieManager = {
		val trieManager = new TrieManager
		trieManager.initializeKeys()
		trieManager.initializeFileSystem(sc)
		trieManager.openTrieBinaries(path)
		trieManager
	}

	def fileName(key: String): String = {
		s"${path}trie_$key.bin"
	}

	def defaultKeys(): List[String] = {
		val upperCaseLetters = (65 to 90).map(_.toChar.toString).toList
		val lowerCaseLetters = (97 to 122).map(_.toChar.toString).toList
		upperCaseLetters ++ lowerCaseLetters ++ List(defaultKey)
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

		val tokenizer = new CleanCoreNLPTokenizer

		val kryoSerializer = new KryoSerializer(conf)
		val files = defaultKeys()
			.map(key => (key, fileName(key)))
			.toMap

		val fileBroadcast = sc.broadcast(files)

		parsedWikipedia
			.mapPartitions({ partition =>
				val trieMap = fileBroadcast.value
					.mapValues { file =>
						val hadoopConf = new Configuration()
						val fs = FileSystem.get(hadoopConf)
						val inputStream = new Input(fs.open(new Path(file)))
						val kryo = kryoSerializer.newKryo()
						val trie = kryo.readObject(inputStream, classOf[TrieNode])
						inputStream.close()
						trie
					}.map(identity)
				partition.map(matchEntry(_, trieMap, tokenizer))
			}, true)
			.saveToCassandra(keyspace, tablename)

		sc.stop
	}
}
