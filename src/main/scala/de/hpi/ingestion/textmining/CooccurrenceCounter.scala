package de.hpi.ingestion.textmining

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Cooccurrence, EntityLink, ParsedWikipediaEntry, Sentence}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Counts the cooccurrences in all sentences.
  */
object CooccurrenceCounter extends SparkJob {
	appName = "Cooccurrence Counter"
	val keyspace = "wikidumps"
	val inputTablename = "wikipediasentences"
	val outputTablename = "wikipediacooccurrences"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val sentences = sc.cassandraTable[Sentence](keyspace, inputTablename)
		List(sentences).toAnyRDD()
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
			.fromAnyRDD[Cooccurrence]()
			.head
			.saveToCassandra(keyspace, outputTablename)
	}

	// $COVERAGE-ON$

	/**
	  * Transforms the entity links into entities and filters duplicate.
	  *
	  * @param sentence Sentence the entities will be extracted from
	  * @return List of unique entities in order of entity link offset
	  */
	def sentenceToEntityList(sentence: Sentence): List[String] = {
		sentence.entities.sortBy(_.offset).map(e => e.entity).distinct
	}

	/**
	  * Groups by entity list and counts cooccurrences.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val sentences = input.fromAnyRDD[Sentence]().head
		val entityLists = sentences
			.map(sentence => (sentenceToEntityList(sentence), 1))
			.filter(_._1.size > 1)
			.reduceByKey(_ + _)
			.map(Cooccurrence.tupled)
		List(entityLists).toAnyRDD()
	}
}
