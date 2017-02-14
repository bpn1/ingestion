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
					for(i <- tokens.indices) {
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
