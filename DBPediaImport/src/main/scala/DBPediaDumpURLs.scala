import org.jsoup.Jsoup
import scala.io.Source
import scala.collection.JavaConverters._

object DBPediaDumpURLs {
	val latestDumpURL = "http://wiki.dbpedia.org/Downloads"

	def main(args: Array[String]) {
		if(args.size < 1) {
			println("Usage: DBPediaDumpURLs path/to/dbpedia.html")
			println(s"Download latest overview from $latestDumpURL")
			System.exit(1)
		}
		val html = Source.fromFile(args(0)).getLines.mkString("\n")
		val doc = Jsoup.parse(html)
		doc
			.select("a:contains(ttl)")
			.asScala
			.map(_.attr("href"))
			.filter(!_.contains("en_uris"))
			.foreach(println)
	}
}
