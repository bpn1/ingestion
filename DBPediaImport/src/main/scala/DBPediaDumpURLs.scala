import org.jsoup.Jsoup
import scala.io.Source
import scala.collection.JavaConverters._

object DBPediaDumpURLs {
	def main(args: Array[String]) {
		val latestDumpURL = "http://wiki.dbpedia.org/Downloads"
		val html = Source.fromURL(latestDumpURL).getLines.mkString("\n")
		val doc = Jsoup.parse(html)
		doc
			.select("a:contains(ttl)")
			.asScala
			.map(_.attr("href"))
			.filter(!_.contains("en_uris"))
			.foreach(println)
	}
}
