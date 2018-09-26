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

package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.framework.SparkJob
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.blockingschemes.{BlockingScheme, SimpleBlockingScheme}
import de.hpi.ingestion.deduplication.Deduplication
import de.hpi.ingestion.deduplication.models.{Candidate, Duplicates}
import de.hpi.ingestion.textmining.models.{EntitySubjectMatch, NERAnnotatedArticle}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class FuzzyMatchingNEL extends SparkJob {
    appName = "Fuzzy Matching NEL"
    configFile = "fuzzy_matching_nel.xml"

    val blockingSchemes = List[BlockingScheme](
        SimpleBlockingScheme("simple_scheme")
    )

    /** Articles that were annotated by an NER. */
    var annotatedArticles: RDD[NERAnnotatedArticle] = _

    /** Subjects that represent our knowledge base. */
    var subjects: RDD[Subject] = _

    /** The matches of annotated entities to Subjects in our knowledge base. */
    var matches: RDD[EntitySubjectMatch] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Trie Alias Articles from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        annotatedArticles = sc.cassandraTable[NERAnnotatedArticle](
            settings("keyspaceNEL"),
            settings("annotatedArticlesTable")
        )
        subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
    }

    /**
      * Saves the found entities to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        matches.saveToCassandra(settings("keyspaceNEL"), settings("matchedEntityTable"))
    }
    // $COVERAGE-ON$

    /**
      * Perform fuzzy matching entity linking by using the Deduplication.
      * @param sc SparkContext to be used for the job
      */
    override def run(sc: SparkContext): Unit = {
        val deduplication = new Deduplication
        deduplication.settings = settings
        deduplication.scoreConfigSettings = scoreConfigSettings
        deduplication.subjects = subjects
        deduplication.stagedSubjects = entitySubjects
        deduplication.blockingSchemes = blockingSchemes
        deduplication.run(sc)
        val duplicates = deduplication.duplicates

        val entityMatches = duplicates.flatMap { case Duplicates(subject_id, subject_name, datasource, candidates) =>
            candidates.map { candidate =>
                EntitySubjectMatch(
                    subject_id = subject_id,
                    subject_name = subject_name,
                    article_id = candidate.id,
                    entity_name = candidate.name.get,
                    article_source = datasource,
                    score = candidate.score
                )
            }
        }

        matches = entityMatches
            .map { entityMatch =>
                ((entityMatch.article_id, entityMatch.entity_name), List(entityMatch))
            }.reduceByKey(_ ++ _)
            .values
            .map(matches => matches.maxBy(_.score))
    }

    /**
      * Transforms the entity annotations into Subjects. The UUID of the article is used as Subject id for every
      * annotated entity in the article. This works since in the Deduplication job there is no "distinct" call on the
      * Subjects.
      *
      * @return RDD of Subjects, which each Subject representing an entity annotation in an article
      */
    def entitySubjects: RDD[Subject] = {
        val datasource = settings("stagingTable")
        annotatedArticles.flatMap { article =>
            val entities = article.entityNames
            entities.map { entityName =>
                Subject(
                    master = article.uuid,
                    id = article.uuid,
                    datasource = datasource,
                    name = Option(entityName)
                )
            }
        }
    }

}
