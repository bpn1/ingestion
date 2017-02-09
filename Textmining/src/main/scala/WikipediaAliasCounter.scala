import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object WikipediaAliasCounter {
	val keyspace = "wikidumps"
	val inputTablename = "wikipedialinks"
	val outputTablename = "wikipediaaliases"

	case class AliasCounter(alias: String, var linkOccurences: Int = 0, var totalOccurences: Int = 0)

	def countAliasOccurences(alias: String): AliasCounter = {
		AliasCounter(alias)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Alias Counter")
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		sc.cassandraTable[WikipediaLinkAnalysis.Link](keyspace, inputTablename)
			.map(link => countAliasOccurences(link.alias))
			.saveToCassandra(keyspace, outputTablename)

		sc.stop()
	}
}
