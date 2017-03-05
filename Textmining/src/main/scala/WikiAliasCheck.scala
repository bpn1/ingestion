import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import WikiClasses._

object WikiAliasCheck {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"

	def checkFoundAlises(entry: ParsedWikipediaEntry): List[Link] = {
		entry.links.filterNot(link => entry.foundaliases.contains(link.alias))
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Alias Counter")
			.set("spark.cassandra.connection.host", "odin01")

		val sc = new SparkContext(conf)

		sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		    .map(entry => (entry.title, checkFoundAlises(entry)))
			.filter(_._2.nonEmpty)
			.saveAsTextFile(s"linkAliasCheck_${System.currentTimeMillis()}")

		sc.stop()
	}
}
