package de.hpi.ingestion.dataimport.wikipedia

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{Text, LongWritable}
import scala.xml.XML
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry

object WikipediaImport {
	val inputFile = "dewiki.xml"
	val keyspace = "wikidumps"
	val tablename = "wikipedia"

	def parseXML(xmlString: String): WikipediaEntry = {
		val xmlDocument = XML.loadString(xmlString)
		val title = (xmlDocument \ "title").text
		val text = Option((xmlDocument \\ "text").text)
		WikipediaEntry(title, text)
	}

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName("WikipediaImport")
		val sc = new SparkContext(conf)
		// scalastyle:off line.size.limit
		// from https://github.com/databricks/spark-xml/blob/master/src/main/scala/com/databricks/spark/xml/util/XmlFile.scala
		// scalastyle:on line.size.limit
		sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
		sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
		sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "UTF-8")
		val inputXML = sc.newAPIHadoopFile(
			inputFile,
			classOf[XmlInputFormat],
			classOf[LongWritable],
			classOf[Text])

		inputXML
			.map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "UTF-8"))
			.map(parseXML)
			.saveToCassandra(keyspace, tablename)
		sc.stop()
	}
}
