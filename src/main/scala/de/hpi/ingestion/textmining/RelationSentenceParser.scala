package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import com.datastax.spark.connector._
import de.hpi.ingestion.textmining.models.{Link, ParsedWikipediaEntry, Sentence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._


object RelationSentenceParser extends SparkJob {
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	//TODO: Create output table
	val outputTablename = "parsedwikipedia"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputTablename)
		List(articles).toAnyRDD()
	}

	/**
	  * Saves Parsed Wikipedia entries with resolved redirects to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[ParsedWikipediaEntry]()
			.head
			.saveToCassandra(keyspace, outputTablename)
	}
	// $COVERAGE-ON$

	/**
	  * TODO
	  * @param text
	  * @param tokenizer
	  * @return
	  */
	def entryToSentencesWithEntitites(
		entry: ParsedWikipediaEntry,
		tokenizer: CoreNLPSentenceTokenizer
	): List[Sentence] = {
		tokenizer.tokenize(entry.getText())
		val text = entry.getText()
		val sentences = tokenizer.tokenize(entry.getText())
		val links = entry.allLinks().filter(_.offset.getOrElse(-1) >= 0)
		var offset = 0
		sentences.map{ sentence =>
			offset = text.indexOf(sentence, offset)
			val sentenceLinks = links.filter { link =>
				link.offset.get > offset && link.offset.get < (offset + sentence.length)
			}
				.map(link => link.copy(offset = link.offset.map(_ - offset)))
			offset += sentence.length
			Sentence(sentence, sentenceLinks)
		}.filter(_.links.length > 1)
	}

	/**
	  * TODO
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		List(articles).toAnyRDD()
	}
}
