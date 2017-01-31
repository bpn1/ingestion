import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.spark.sql.{SQLContext, cassandra, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.util.matching.Regex
import info.bliki.wiki.model.WikiModel
import WikipediaTextparser._
import org.jsoup.Jsoup
import com.datastax.spark.connector._



object WikipediaRedirects {
	val inputFile = "dewiki.xml" // load from hdfs
	val tablename = "parsedwikipedia"
	val keyspace = "wikidumps"

	val wikiSchema = StructType(Array(
		StructField("title", StringType),
		StructField("revision", StructType(Array(
			StructField("text", StringType))))))

	def wikipediaToHtml(wikipediaMarkup: String): String = {
		val html = wikipediaMarkup.replaceAll("\\\\n", "\n")
		WikiModel.toHtml(removeWikiMarkup(html))
	}

	def parseRedirect(title: String, html: String): ParsedWikipediaEntry = {
		val parsedEntry = new ParsedWikipediaEntry(title, Option(""), null)
		val doc = Jsoup.parse(html)
		val text = doc.body.text.replaceAll("(\\AWEITERLEITUNG)|(\\AREDIRECT)", "REDIRECT")
		parsedEntry.setText(text)
		parsedEntry.links = extractRedirect(html)
		return parsedEntry

	}
	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("WikipediaRedirectsImport")
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)
		val sql = new SQLContext(sc)

		var df = sql.read
			.format("com.databricks.spark.xml")
			.option("rowTag", "page")
			.schema(wikiSchema)
			.load(inputFile)

		df = df.withColumn("text", df("revision.text")).drop("revision")
		val redirectRegex = new Regex("(#WEITERLEITUNG)|(#REDIRECT)")
		val wikiRDD = df
			.rdd
			.map(_.toSeq)
			.map{ list =>
				val text = Option(list(1).asInstanceOf[String]) match {
					case Some(t) => t
					case None => ""
				}
				(list(0).asInstanceOf[String], text)
			}.filter{ case (title, text: String) =>
				redirectRegex.findFirstIn(text) != None
			}.map(entry => (entry._1, wikipediaToHtml(entry._2)))
			.map(entry => parseRedirect(entry._1, entry._2))
			.saveToCassandra(keyspace, tablename)


		sc.stop()
	}
}
