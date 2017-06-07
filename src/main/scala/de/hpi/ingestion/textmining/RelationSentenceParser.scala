package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import com.datastax.spark.connector._
import de.hpi.ingestion.textmining.models.{EntityLink, ParsedWikipediaEntry, Sentence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

/**
  * Parses all Sentences with entities from Wikipedia.
  */
object RelationSentenceParser extends SparkJob {
	appName = "Relation Sentence Parser"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		List(articles).toAnyRDD()
	}

	/**
	  * Saves Sentences with entities to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Sentence]()
			.head
			.saveToCassandra(settings("keyspace"), settings("sentenceTable"))
	}

	// $COVERAGE-ON$

	/**
	  * Parses all Sentences from a parsed Wikipedia etry with at least 2 entities
	  * and recalculating relative offsets of entities.
	  *
	  * @param entry     Parsed Wikipedia Entry to be processed into sentences
	  * @param tokenizer tokenizer to be used
	  * @return List of Sentences with at least two entities
	  */
	def entryToSentencesWithEntities(
		entry: ParsedWikipediaEntry,
		tokenizer: IngestionTokenizer
	): List[Sentence] = {
		val text = entry.getText()
		val sentences = tokenizer.process(text)
		val links = entry.allLinks().filter(_.offset.exists(_ >= 0))
		var offset = 0
		sentences.map { sentence =>
			offset = text.indexOf(sentence, offset)
			val sentenceEntities = links.filter { link =>
				link.offset.exists(_ >= offset) && link.offset.exists(_ < offset + sentence.length)
			}
				.map(link => EntityLink(link.alias, link.page, link.offset.map(_ - offset)))
			offset += sentence.length
			Sentence(entry.title, offset-sentence.length, sentence, sentenceEntities)
		}.filter(_.entities.length > 1)
	}

	/**
	  * Parses all Sentences with at least 2 entities from every Wikipedia article and puts them into an RDD.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val tokenizer = IngestionTokenizer(Array("SentenceTokenizer", "false", "false"))
		val sentences = articles.flatMap(entry => entryToSentencesWithEntities(entry, tokenizer))
		List(sentences).toAnyRDD()
	}
}
