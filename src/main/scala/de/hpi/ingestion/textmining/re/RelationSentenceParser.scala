/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.re

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{EntityLink, ParsedWikipediaEntry, Sentence}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Parses all `Sentences` with at least two entities from each Wikipedia article and writes them to the Cassandra.
  */
object RelationSentenceParser extends SparkJob {
	appName = "Relation Sentence Parser"
	cassandraSaveQueries += "TRUNCATE TABLE wikidumps.wikipediasentences"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries and DBpedia Relations from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val relations = sc.cassandraTable[Relation](settings("keyspace"), settings("DBpediaRelationTable"))
		List(articles).toAnyRDD() ++ List(relations).toAnyRDD()
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
	  * Parses all Sentences from a parsed Wikipedia entry with at least 2 entities
	  * and recalculating relative offsets of entities.
	  *
	  * @param entry     Parsed Wikipedia Entry to be processed into sentences
	  * @param tokenizer tokenizer to be used
	  * @return List of Sentences with at least two entities
	  */
	def entryToSentencesWithEntities(
		entry: ParsedWikipediaEntry,
		sentenceTokenizer: IngestionTokenizer,
		tokenizer: IngestionTokenizer
	): List[Sentence] = {
		val text = entry.getText()
		val sentences = sentenceTokenizer.process(text)
		val links = entry.reducedLinks().filter(_.offset.exists(_ >= 0)).distinct
		var offset = 0
		sentences.map { sentence =>
			offset = text.indexOf(sentence, offset)
			val sentenceEntities = links.filter { link =>
				link.offset.exists(_ >= offset) && link.offset.exists(_ + link.alias.length <= offset + sentence.length)
			}
				.map(link => EntityLink(link.alias, link.page, link.offset.map(_ - offset)))
				.sortBy(_.offset)
			offset += sentence.length
			var sentenceOffset = 0
			var bagOfWords = List.empty[String]
			sentenceEntities
				.sortBy(_.offset)
				.foreach { entity =>
					entity.offset.foreach { entityOffset =>
						bagOfWords ++= tokenizer.process(sentence.substring(sentenceOffset, entityOffset))
						sentenceOffset = entityOffset + entity.alias.length
					}
				}
			bagOfWords ++= tokenizer.process(sentence.substring(sentenceOffset, sentence.length))
			Sentence(entry.title, offset-sentence.length, sentence, sentenceEntities, bagOfWords.filter(_.nonEmpty))
		}.filter(_.entities.length > 1)
	}

	/**
	  * Filters Entities of Sentences from countries or cities (non companies)
	  * and removes the sentences with less than 1 entity afterwards
	  *
	  * @param sentences sentences to be filtered
	  * @param relations relations to be filtered by
	  * @param sc        Sparkcontext needed for broadcasting
	  * @return filtered List
	  */
	def filterSentences(sentences: RDD[Sentence], relations: RDD[Relation], sc: SparkContext): RDD[Sentence] = {
		val relationtypes = Set(
			"country",
			"capital",
			"location",
			"city",
			"birthPlace",
			"district",
			"federalState",
			"state"
		)

		val blacklist = relations.filter(relation => relationtypes.contains(relation.relationtype))
			.map(_.objectentity)
			.collect
			.toSet
		val blacklistBroadcast = sc.broadcast(blacklist)
		sentences.mapPartitions({ entryPartition =>
			val localBlacklist = blacklistBroadcast.value
			entryPartition.map { sentence =>
				val filteredEntities = sentence
					.entities
					.filter(entity => !localBlacklist.contains(entity.entity))
					.sortBy(_.offset)
				sentence.copy(entities = filteredEntities)
			}
		}).filter(_.entities.length > 1)
	}

	/**
	  * Parses all `Sentences` with at least two entities from each Wikipedia article.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val articles = input.fromAnyRDD[ParsedWikipediaEntry]().head
		val relations = input.fromAnyRDD[Relation]().last

		val sentenceTokenizer = IngestionTokenizer(Array("SentenceTokenizer", "false", "false"))
		val tokenizer = IngestionTokenizer(Array("CleanWhitespaceTokenizer", "false", "false"))
		val sentences = articles.flatMap(entryToSentencesWithEntities(_, sentenceTokenizer, tokenizer))

		val filteredSentences = filterSentences(sentences, relations, sc)
		List(filteredSentences).toAnyRDD()
	}
}
