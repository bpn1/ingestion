
import org.jsoup.Jsoup
import org.jsoup.select._
import org.jsoup.nodes._
import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object DBPediaDumpURLs {
	def main(args: Array[String]) {
		val html = Source.fromFile("dbpedia2016.html").getLines.mkString
		val doc = Jsoup.parse(html)
		val links = doc.select("a:contains(ttl)").asScala.map(_.attr("href")).filter(!_.contains("en_uris")).foreach(println)
		}
}
