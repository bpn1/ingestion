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

package de.hpi.ingestion.dataimport.dbpedia

import java.util.UUID
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Import and add the dbpedia relations to the dbpedia subjects.
  */
class DBpediaRelationImport extends SparkJob {
    import DBpediaRelationImport._
    appName = "DBpediaRelationImport"
    configFile = "relation_import_dbpedia.xml"

    var subjects: RDD[Subject] = _
    var relations: RDD[Relation] = _
    var subjectsWithRelations: RDD[Subject] = _

    // $COVERAGE-OFF$
    /**
      * Loads DBpedia Subjects and DBpedia relations
      * @param sc   Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        subjects = sc.cassandraTable[Subject](settings("keyspaceDBpediaTable"), settings("dbpediaTable"))
        relations = sc.cassandraTable[Relation](
            settings("keyspaceDBpediaRelationsTable"),
            settings("dbpediaRelationsTable"))
    }

    /**
      * Saves the DBpedia Subjects to the Cassandra.
      * @param sc     Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        subjectsWithRelations.saveToCassandra(settings("keyspaceDBpediaTable"), settings("dbpediaTable"))
    }
    // $COVERAGE-ON$

    /**
      * Enriches the DBpedia Subjects with DBpedia relations.
      * @param sc SparkContext to be used for the job
      */
    override def run(sc: SparkContext): Unit = {
        val version = Version("DBpediaRelationImport", List("dbpedia"), sc, false, settings.get("dbpediaTable"))

        val dbpediaByName = subjects.flatMap(subject => subject.name.map((_, subject)))

        subjectsWithRelations = relations
            .keyBy(_.objectentity)
            .join(dbpediaByName)
            .values
            .map { case (Relation(subjectEntity, relationType, objectEntity), subject) =>
                (subjectEntity, List((relationType, subject.id)))
            }.reduceByKey(_ ::: _)
            .join(dbpediaByName)
            .values
            .map { case (subjectRelations, subject) =>
                addRelations(subject, subjectRelations, version)
                subject
            }
    }
}

object DBpediaRelationImport {
    /**
      * Adds the relation defined in the relation object to the subject
      * @param subjectEntity subject to which the relation should be added
      * @param relations relation type and objectEntity Id
      * @param version version of the Import
      */
    def addRelations(subjectEntity: Subject, relations: List[(String, UUID)], version: Version): Unit = {
        val subjectManager = new SubjectManager(subjectEntity, version)

        val newRelations = relations
            .groupBy(_._2)
            .mapValues(_.map(_._1 -> "").toMap)

        subjectManager.addRelations(newRelations)
    }
}
