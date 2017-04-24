package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import com.datastax.spark.connector._
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.serializers.FieldSerializer
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
	  * Finds all occurrences of aliases in the text of a Wikipedia entry
	  * @param entry parsed Wikipedia entry to use
	  * @param trie Partitioned Trie containing the aliases we look for
	  * @param tokenizer Tokenizer used to tokenize the text of the entry
	  * @return entry containing list of found aliases
	  */
	def matchEntry(
		entry: ParsedWikipediaEntry,
		//trieMap: Map[String, TrieNode],
		trie: TrieNode,
		tokenizer: Tokenizer
	): ParsedWikipediaEntry = {
		val resultList = mutable.ListBuffer[String]()
		val tokens = tokenizer.tokenize(entry.getText())
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
//			val key = tokens.headOption
//				.filter(_.nonEmpty)
//				.map(_.charAt(0))
//				.filter(_.isASCIILetter)
//				.map(_.toString)
//				.getOrElse(defaultKey)
//			val aliasMatches = trieMap(key).matchTokens(testTokens)
			val aliasMatches = trie.matchTokens(testTokens)
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

	def trieKryo(): Kryo = {
		val kryo = new Kryo()
		kryo.register(classOf[mutable.Map[_, _]], new MutableMapSerializer())
		kryo.register(classOf[Option[_]], new OptionSerializer())

		val trieSerializer = new FieldSerializer(kryo, classOf[TrieNode])
		trieSerializer
			.getField("children")
			.setClass(
				classOf[mutable.Map[_, _]],
				kryo.getSerializer(classOf[mutable.Map[_, _]]))
		trieSerializer
			.getField("token")
			.setClass(
				classOf[Option[_]],
				kryo.getSerializer(classOf[Option[_]]))
		trieSerializer
			.getField("word")
			.setClass(
				classOf[Option[_]],
				kryo.getSerializer(classOf[Option[_]]))
		kryo.register(classOf[TrieNode], trieSerializer)
		kryo
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

		val tokenizer = new CoreNLPTokenizer

//		val files = defaultKeys()
//			.map(key => (key, fileName(key)))
//			.toMap
//
//		val fileBroadcast = sc.broadcast(files)

		parsedWikipedia
//			.coalesce(16)
			.mapPartitions({ partition =>
//				val trieMap = fileBroadcast.value
//					.mapValues { file =>
//						val kryo = trieKryo()
//						val hadoopConf = new Configuration()
//						val fs = FileSystem.get(hadoopConf)
//						val inputStream = new Input(fs.open(new Path(file)))
//
//						val trie = kryo.readObject(inputStream, classOf[TrieNode])
//						inputStream.close()
//						trie
//					}.map(identity)
				val kryo = trieKryo()
				val hadoopConf = new Configuration()
				val fs = FileSystem.get(hadoopConf)
				val inputStream = new Input(fs.open(new Path("trie_full.bin")))
				val trie = kryo.readObject(inputStream, classOf[TrieNode])
				inputStream.close()
				partition.map(matchEntry(_, trie, tokenizer))
			}, true)
			.saveToCassandra(keyspace, tablename)

		sc.stop
	}
}
