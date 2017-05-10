package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import com.datastax.spark.connector._
import de.hpi.ingestion.textmining.models.{Entity, Link, ParsedWikipediaEntry, Sentence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.AliasTrieSearch.appName

/**
  * Parses all Sentences with entities from Wikipedia
  */
object RelationSentenceParser extends SparkJob {
	appName = "Relation Sentence Parser"
	val keyspace = "wikidumps"
	val inputTablename = "parsedwikipedia"
	val outputTablename = "wikipediasentences"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputTablename)
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
			.saveToCassandra(keyspace, outputTablename)
	}

	// $COVERAGE-ON$

	/**
	  * Parses all Sentences from a parsed Wikipedia etry with at least 2 entities
	  * and recalculating relative offsets of entities
	  *
	  * @param entry     Parsed Wikipedia Entry to be processed into sentences
	  * @param tokenizer tokenizer to be used
	  * @return List of Sentences with at least two entities
	  */
	def entryToSentencesWithEntities(
		entry: ParsedWikipediaEntry,
		tokenizer: CoreNLPSentenceTokenizer
	): List[Sentence] = {
		tokenizer.tokenize(entry.getText())
		val text = entry.getText()
		val sentences = tokenizer.tokenize(entry.getText())
		val links = entry.allLinks().filter(_.offset.getOrElse(-1) >= 0).distinct
		var offset = 0
		sentences.map { sentence =>
			offset = text.indexOf(sentence, offset)
			val sentenceEntities = links.filter { link =>
				link.offset.get >= offset && link.offset.get < (offset + sentence.length)
			}
				.map(link => Entity(link.alias, link.page, link.offset.map(_ - offset)))
			offset += sentence.length
			Sentence(entry.title, offset-sentence.length, sentence, sentenceEntities)
		}.filter(_.entities.length > 1)
	}

	/**
	  * Parses all Sentences with at least 2 entities from every Wikipedia article and puts them into an RDD
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val tokenizer = new CoreNLPSentenceTokenizer()
		val sentences = articles.flatMap(entry => entryToSentencesWithEntities(entry, tokenizer))
		List(sentences).toAnyRDD()
	}
}
