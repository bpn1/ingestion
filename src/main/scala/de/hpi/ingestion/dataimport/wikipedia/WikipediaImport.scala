package de.hpi.ingestion.dataimport.wikipedia

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.databricks.spark.xml.XmlInputFormat
import org.apache.hadoop.io.{LongWritable, Text}

import scala.xml.XML
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd.RDD

object WikipediaImport extends SparkJob {
	appName = "WikipediaImport"
	val inputFile = "dewiki.xml"
	val keyspace = "wikidumps"
	val tablename = "wikipedia"

	// $COVERAGE-OFF$
	/**
	  * Reads Wikipedia XML dump from HDFS.
	  * Config source: https://git.io/v9q24
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		sc.hadoopConfiguration.set(XmlInputFormat.START_TAG_KEY, "<page>")
		sc.hadoopConfiguration.set(XmlInputFormat.END_TAG_KEY, "</page>")
		sc.hadoopConfiguration.set(XmlInputFormat.ENCODING_KEY, "UTF-8")
		val inputXML = sc.newAPIHadoopFile(inputFile, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text])
			.map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, "UTF-8"))
		List(inputXML).toAnyRDD()
	}

	/**
	  * Saves Wikipedia Entries to Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[WikipediaEntry]()
			.head
			.saveToCassandra(keyspace, tablename)
	}
	// $COVERAGE-ON$
	/**
	  * Parses Wikipedia XML to Wikipedia entries.
	  * @param xmlString XML data as String
	  * @return Wikipedia Entry containing the title and text
	  */
	def parseXML(xmlString: String): WikipediaEntry = {
		val xmlDocument = XML.loadString(xmlString)
		val title = (xmlDocument \ "title").text
		val text = Option((xmlDocument \\ "text").text)
		WikipediaEntry(title, text)
	}

	/**
	  * Parses Wikipedia XML dump to Wikipedia Entries.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		input
			.fromAnyRDD[String]()
			.map(_.map(parseXML))
			.toAnyRDD()
	}
}
