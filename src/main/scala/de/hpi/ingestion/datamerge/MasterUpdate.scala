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

package de.hpi.ingestion.datamerge

import java.util.UUID

import de.hpi.ingestion.framework.SparkJob
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONParser
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsValue, Json}

class MasterUpdate extends SparkJob {
    import MasterUpdate._
    appName = "Master Update"
    configFile = "master_update.xml"

    var subjects: RDD[Subject] = _
    var updatedMasters: RDD[Subject] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Subjects from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        subjects = sc.cassandraTable[Subject](settings("subjectKeyspace"), settings("subjectTable"))
    }

    /**
      * Writes the updated master nodes to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        updatedMasters.saveToCassandra(settings("subjectKeyspace"), settings("subjectTable"))
    }
    // $COVERAGE-ON$

    /**
      * Updates the master nodes by newly generating their data.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val version = Version(appName, List("master update"), sc, true, settings.get("subjectTable"))
        val masterGroups = updateSubjects(subjects, conf.commitJsonOpt)
            .map(subject => (subject.master, List(subject)))
            .reduceByKey(_ ++ _)
            .values
        updatedMasters = masterGroups.map { masterGroup =>
            val master = masterGroup.find(_.isMaster).get
            val slaves = masterGroup.filter(_.isSlave)
            Merging.mergeIntoMaster(master, slaves, version).head
        }
    }
}

object MasterUpdate extends JSONParser {
    /**
      * Parses the Commit JSON Object to extract the ids of the changed master Subjects.
      * @param commitJson the JSON of the Commit job. Contains all changes done in the commit
      * @return Set of master ids that will be updated
      */
    def getMastersFromCommit(commitJson: JsValue): Set[UUID] = {
        (extractMap(commitJson, List("created")).keySet ++
            extractMap(commitJson, List("updated")).keySet ++
            extractMap(commitJson, List("deleted")).keySet
            ).map(UUID.fromString(_))
    }

    /**
      * Filters the passed Subjects for only those that need to be updated, if this job is executed after a Commit job.
      * @param subjects RDD of Subjects that will be filtered
      * @param commitConfOption command line option of curation commit JSON object that contains the changed Subjects
      * @return the Subjects that need to be updated, if the JSON of a Commit job is provided. Otherwise the input
      * Subjects are returned
      */
    def updateSubjects(subjects: RDD[Subject], commitConfOption: Option[String] = None): RDD[Subject] = {
        commitConfOption
            .map(Json.parse)
            .map(getMastersFromCommit)
            .map(masterIds => subjects.filter(subject => masterIds(subject.master)))
            .getOrElse(subjects)
    }
}
