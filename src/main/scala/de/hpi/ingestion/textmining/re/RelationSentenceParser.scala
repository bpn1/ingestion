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
import de.hpi.ingestion.dataimport.wikidata.models.WikidataEntity
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{EntityLink, ParsedWikipediaEntry, Sentence}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.preprocessing.CompanyLinkFilter.extractCompanyPages
import de.hpi.ingestion.textmining.tokenizer.{CleanWhitespaceTokenizer, IngestionTokenizer, SentenceTokenizer}

/**
  * Parses all `Sentences` with at least two entities from each Wikipedia article and writes them to the Cassandra.
  */
class RelationSentenceParser extends SparkJob {
    import RelationSentenceParser._
    appName = "Relation Sentence Parser"
    cassandraSaveQueries += "TRUNCATE TABLE wikidumps.wikipediasentences"
    configFile = "textmining.xml"

    var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
    var relations: RDD[Relation] = _
    var wikidataEntries: RDD[WikidataEntity] = _
    var sentences: RDD[Sentence] = _

    // $COVERAGE-OFF$
    /**
      * Loads Parsed Wikipedia entries and DBpedia Relations from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
        relations = sc.cassandraTable[Relation](settings("keyspace"), settings("DBpediaRelationTable"))
        wikidataEntries = sc.cassandraTable[WikidataEntity](settings("keyspace"), settings("wikidataTable"))
    }

    /**
      * Saves Sentences with entities to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        sentences.saveToCassandra(settings("keyspace"), settings("sentenceTable"))
    }
    // $COVERAGE-ON$

    /**
      * Parses all `Sentences` with at least two entities from each Wikipedia article.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val companies = extractCompanyPages(wikidataEntries).collect.toSet
        val sentenceTokenizer = IngestionTokenizer(new SentenceTokenizer, false, false)
        val tokenizer = IngestionTokenizer(new CleanWhitespaceTokenizer, false, true)
        val parsedSentences = parsedWikipedia.flatMap(entryToSentencesWithEntities(_, sentenceTokenizer, tokenizer))
        sentences = filterSentences(parsedSentences, relations, companies, sc)
    }
}

object RelationSentenceParser {
    /**
      * Parses all Sentences from a parsed Wikipedia entry with at least 2 entities
      * and recalculating relative offsets of entities.
      *
      * @param entry     Parsed Wikipedia Entry to be processed into sentences
      * @param sentenceTokenizer tokenizer that splits a text into sentences
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
      * or which contain an entity in our blacklist
      * and removes the sentences with less than 1 entity afterwards
      *
      * @param sentences sentences to be filtered
      * @param relations relations to be filtered by
      * @param sc        Sparkcontext needed for broadcasting
      * @return filtered List
      */
    def filterSentences(
        sentences: RDD[Sentence],
        relations: RDD[Relation],
        companies: Set[String],
        sc: SparkContext
    ): RDD[Sentence] = {
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

        val handmadeBlacklist = Set(
            "Hörfunk",
            "Fernsehen",
            "Einzelhandel",
            "Großhandel",
            "Landwirtschaft",
            "Industrie"
        )
        val blacklistBroadcast = sc.broadcast(blacklist)
        val whiteListBroadcast = sc.broadcast(companies)
        sentences.mapPartitions({ entryPartition =>
            val localBlacklist = blacklistBroadcast.value
            val localWhitelist = whiteListBroadcast.value
            entryPartition.map { sentence =>
                val filteredEntities = sentence
                    .entities
                    .filter { entity =>
                        !localBlacklist.contains(entity.entity) &&
                            !handmadeBlacklist.contains(entity.entity) &&
                            localWhitelist.contains(entity.entity)
                    }.sortBy(_.offset)
                sentence.copy(entities = filteredEntities)
            }
        }).filter(_.entities.length > 1)
    }
}
