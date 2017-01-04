import org.jsoup.Jsoup
import scala.io.Source
import scala.collection.JavaConverters._

object DBPediaDumpURLs {
	def main(args: Array[String]) {
		val html = Source.fromFile("dbpedia2016.html").getLines.mkString
		val doc = Jsoup.parse(html)
		val links = doc.select("a:contains(ttl)").asScala.map(_.attr("href")).filter(!_.contains("en_uris")).foreach(println)
		}
}
