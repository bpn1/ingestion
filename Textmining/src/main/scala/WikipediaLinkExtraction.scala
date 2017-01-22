import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

object WikipediaLinkExtraction {
	val keyspace = "wikidumps"
	val tablename = "wikipedia"

	case class WikipediaReadEntry(
		title: String,
		var text: Option[String],
		var references: Map[String, List[String]])

	case class WikipediaEntry(
		title: String,
		var text: String,
		var references: Map[String, List[String]])

	def extractLinks(entry : WikipediaEntry): WikipediaEntry = {
		val html = Jsoup.parse(entry.text)
		entry.references = getLinks(html)
		entry
	}

	def getLinks(html: Document): Map[String, List[String]] = {
		val links = mutable.Map[String, mutable.ListBuffer[String]]()
		val validAnchors = html
			.select("a")
			.filter(_.getElementsByAttributeValueStarting("title", "Datei:").size == 0)
		for(anchor <- validAnchors) {
			val source = anchor.text
			var target = anchor.attr("href")
			links(source) = links.getOrElse(source, mutable.ListBuffer[String]()) += target
		}
		links.mapValues(_.toList).toMap
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("Wikipedia Link Extraction")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val wikipedia = sc.cassandraTable[WikipediaReadEntry](keyspace, tablename)
		wikipedia
			.map { entry =>
				val text = entry.text match {
					case Some(string) => string
					case None => ""
				}
				WikipediaEntry(entry.title, text, entry.references)
			}.map(extractLinks)
			.saveToCassandra(keyspace, tablename)
		sc.stop
	}
}
