import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

import scala.collection.mutable
import WikiClasses._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.FieldSerializer
import com.esotericsoftware.kryo.io.Input

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object TrieBuilder {
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"

	def matchEntry(entry: ParsedWikipediaEntry, trie: TrieNode, tokenizer: Tokenizer): ParsedWikipediaEntry = {
		val sentenceTokenizer = new SentenceTokenizer()
		val sentenceList = sentenceTokenizer.tokenize(entry.getText)
		val resultList = mutable.ListBuffer[String]()
		for(sentence <- sentenceList) {
			val tokens = tokenizer.tokenize(sentence)
			for(i <- tokens.indices) {
				val testTokens = tokens.slice(i, tokens.length)
				val aliasMatches = trie.matchTokens(testTokens)
				resultList ++= aliasMatches.map(tokenizer.reverse)
			}
		}
		entry.foundaliases = resultList.toList
		entry
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("TrieBuilder")
			.set("spark.cassandra.connection.host", "odin01")
		 	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val sc = new SparkContext(conf)

		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
//		val aliases = parsedWikipedia
//			.flatMap(_.links)
//			.map(_.alias)
//			.distinct
//			.collect

		val tokenizer = new WhitespaceTokenizer()

		parsedWikipedia
			.mapPartitions({ partition =>
				val kryo = new Kryo()
				kryo.addDefaultSerializer(classOf[mutable.Map[_, _]], classOf[MutableMapSerializer])
				kryo.addDefaultSerializer(classOf[Option[_]], classOf[OptionSerializer])

				val trieSerializer = new FieldSerializer(kryo, classOf[TrieNode])
				trieSerializer.getField("children").setClass(classOf[mutable.Map[_, _]], kryo.getSerializer(classOf[mutable.Map[_, _]]))
				trieSerializer.getField("token").setClass(classOf[Option[_]], kryo.getSerializer(classOf[Option[_]]))
				trieSerializer.getField("word").setClass(classOf[Option[_]], kryo.getSerializer(classOf[Option[_]]))
				kryo.register(classOf[TrieNode], trieSerializer)
				val hadoopConf = new Configuration()
				val triePath = new Path("/user/bp2016n1/trie.bin")
				val fs = FileSystem.get(hadoopConf)
				val trieStream = fs.open(triePath)
				val trieInput = new Input(trieStream)
				val inputTrie = kryo.readObject(trieInput, classOf[TrieNode])
				trieInput.close()
				partition.map(entry => matchEntry(entry, inputTrie, tokenizer))
			}, true)
			.saveToCassandra(keyspace, tablename)

		sc.stop
	}
}
