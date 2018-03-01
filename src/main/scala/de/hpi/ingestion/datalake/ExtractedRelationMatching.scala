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

package de.hpi.ingestion.datalake

import de.hpi.ingestion.datalake.models.{ExtractedRelation, ImportRelation, Subject}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

class ExtractedRelationMatching extends SparkJob {
    appName = "Extracted Relation Matching"
    configFile = "datalake_relation_matching.xml"

    var subjects: RDD[Subject] = _
    var relations: RDD[ExtractedRelation] = _

    var matchedRelations: RDD[ImportRelation] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Subjects and extracted relations to import from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        subjects = sc.cassandraTable[Subject](settings("subjectKeyspace"), settings("subjectTable"))
        relations = sc.cassandraTable[ExtractedRelation](settings("relationKeyspace"), settings("relationTable"))
    }

    /**
      * Saves the matched relations to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        matchedRelations.saveToCassandra(settings("outputKeyspace"), settings("outputTable"))
    }
    // $COVERAGE-ON$

    override def run(sc: SparkContext): Unit = {
        val entities = relations.flatMap(relation => List(relation.start, relation.destination))
        val subjectMapping = subjects
            .filter(_.isSlave)
            .flatMap(subject => (subject.name.toList ++ subject.aliases).map((_, (subject.id, subject.master))))
        val nameToIdsWithErrors = subjectMapping
            .join(entities.map(name => Tuple2(name, name)))
            .values
            .map { case ((id, master), name) => (name, List((id, master))) }
            .reduceByKey(_ ++ _)
            .map {
                case (name, matchedSubjects) if matchedSubjects.length == 1 =>
                    ((name, matchedSubjects.head._1), true)
                case (name, matchedSubjects) if matchedSubjects.length > 1 =>
                    val duplicateMatches = matchedSubjects.map(_._2).distinct.length == 1
                    if(duplicateMatches) {
                        ((name, matchedSubjects.head._1), true)
                    } else {
                        ((name, null), false)
                    }
            }
        val badMatches = nameToIdsWithErrors
            .collect { case ((name, null), false) => name }
            .collect
        if(badMatches.nonEmpty) {
            println("Names with multiple matches:")
            badMatches.foreach(println)
        }

        val nameToId = nameToIdsWithErrors
            .collect { case ((name, id), true) => (name, id) }
            .collect
            .toMap
        val nameToIdBroadcast = sc.broadcast(nameToId)
        matchedRelations = relations.mapPartitions({ partition =>
            val nameToIdMap = nameToIdBroadcast.value
            partition.flatMap { relation =>
                val start = nameToIdMap.get(relation.start)
                val destination = nameToIdMap.get(relation.destination)
                start.flatMap { startId =>
                    destination.map(destinationId => ImportRelation(startId, destinationId, relation.relation))
                }
            }
        }, true)
    }
}
