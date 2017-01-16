import org.sweble.wikitext._
import org.sweble.wikitext.engine._
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.sweble.wikitext.engine.utils._
import org.sweble.wikitext.engine.nodes._
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.sweble.wikitext.parser.nodes._
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel;

object WikipediaTextparser {
    val keyspace = "wikidumps"
    val tablename = "wikipedia"

    case class WikipediaEntry(
        title: String,
        var text: String,
        var links: List[Map[String, String]])

	def wikipediaToHtml(entry: WikipediaEntry): WikipediaEntry = {
		val cleanText = entry.text.replaceAll("\\\\n", "\n")
		entry.text = WikiModel.toHtml(cleanText)
		entry
	}
	def cleanTables(entry: WikipediaEntry): WikipediaEntry = {
		val tableRegex = new Regex("<table(.*\n)*?.*?</table>")
	}


    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setAppName("VersionDiff")
            .set("spark.cassandra.connection.host", "172.20.21.11")
        val sc = new SparkContext(conf)

        val wikipedia = sc.cassandraTable[WikipediaEntry](keyspace, tablename)
		/*
		wikipedia
            .map(wikipediaToHtml)
			.saveToCassandra(keyspace, tablename)
		*/
		sc.stop
    }
}
