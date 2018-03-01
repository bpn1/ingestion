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

import de.hpi.ingestion.datalake.models.{ImportRelation, Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

class RelationImport extends SparkJob {
    appName = "Relation Import"
    configFile = "datalake_relation_import.xml"

    var subjects: RDD[Subject] = _
    var relations: RDD[ImportRelation] = _

    var updatedSubjects: RDD[Subject] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Subjects and Relations to import from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        subjects = sc.cassandraTable[Subject](settings("subjectKeyspace"), settings("subjectTable"))
        relations = sc.cassandraTable[ImportRelation](settings("relationKeyspace"), settings("relationTable"))
    }

    /**
      * Saves the updates Subjects to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        updatedSubjects.saveToCassandra(settings("outputKeyspace"), settings("outputTable"))
    }
    // $COVERAGE-ON$

    override def run(sc: SparkContext): Unit = {
        val version = Version(appName, Nil, sc, false, settings.get("outputTable"))
        val groupedRelations = relations
            .map(relation => (relation.start, List(relation)))
            .reduceByKey(_ ++ _)
        updatedSubjects = subjects
            .map(subject => (subject.id, subject))
            .join(groupedRelations)
            .values
            .map { case (subject, importRelations) =>
                val subjectManager = new SubjectManager(subject, version)
                importRelations.foreach { relation =>
                    val relationMap = Map(relation.destination -> Map(relation.relation -> ""))
                    subjectManager.addRelations(relationMap)
                }
                subject
            }
    }
}
