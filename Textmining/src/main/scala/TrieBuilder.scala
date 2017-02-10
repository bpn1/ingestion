import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.mutable
import WikiClasses._

object TrieBuilder {
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("TrieBuilder")
			.set("spark.cassandra.connection.host", "odin01")
		// 	.set("spark.kryo.registrationRequired", "true")
		// 	.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		// 	.set("spark.kryo.classesToRegister", "Trie,TrieNode")
		// conf.registerKryoClasses(Array(classOf[Seq[String]], classOf[List[String]], classOf[Array[String]]))
		val sc = new SparkContext(conf)

		val parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
		val aliases = parsedWikipedia
			.flatMap(_.links)
			.map(_.alias)
			.distinct
			.collect
		val trie = new TrieNode()
		val tokenizer = new WhitespaceTokenizer()
		for(alias <- aliases) {
			val tokens = tokenizer.tokenize(alias)
			trie.append(tokens)
		}
		val trieBroadcast = sc.broadcast(trie)

		parsedWikipedia
			.map { entry =>
				val localTrie = trieBroadcast.value
				val sentenceTokenizer = new SentenceTokenizer()
				val sentenceList = sentenceTokenizer.tokenize(entry.getText)
				val resultList = mutable.ListBuffer[String]()

				for(sentence <- sentenceList) {
					val tokens = tokenizer.tokenize(sentence)
					for(i <- 0 until tokens.length) {
						val aliasMatches = localTrie.matchTokens(tokens.slice(i, tokens.length))
						resultList ++= aliasMatches.map(tokenizer.reverse)
					}
				}
				entry.foundaliases = resultList.toList
				entry
			}.saveToCassandra(keyspace, tablename)


		sc.stop
	}
}
