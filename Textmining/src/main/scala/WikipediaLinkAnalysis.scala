import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object WikipediaLinkAnalysis {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputTablename = "wikipedialinks"

	def groupAliasesByPageNames(parsedWikipedia: RDD[WikipediaTextparser.ParsedWikipediaEntry]): RDD[(String, Iterable[(String, String)])] = {
		parsedWikipedia
			.flatMap(_.links)
			.map(link => (link.alias, link.page))
			.groupBy(_._2)
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Link Extraction")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val parsedWikipedia = sc.cassandraTable[WikipediaTextparser.ParsedWikipediaEntry](keyspace, inputTablename)
		groupAliasesByPageNames(parsedWikipedia)
			.saveToCassandra(keyspace, outputTablename)
		sc.stop
	}
}
