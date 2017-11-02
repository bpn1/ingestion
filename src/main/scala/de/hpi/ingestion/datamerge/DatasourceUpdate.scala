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

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Updates the current Subjects with the data of an already integrated datasource. The new data is added to the
  * already existing Subjects representing the same entity of the given datasource.
  */
class DatasourceUpdate extends SparkJob {
    import DatasourceUpdate._
    appName = "Datasource Update"
    configFile = "datasource_update.xml"

    var subjects: RDD[Subject] = _
    var subjectsWithUpdate: RDD[Subject] = _
    var updatedSubjects: RDD[Subject] = _

    // $COVERAGE-OFF$
    /**
      * Loads the new Subjects used to update the current Subjects and the current Subjects.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        subjectsWithUpdate = sc.cassandraTable[Subject](settings("inputKeyspace"), settings("inputTable"))
        subjects = sc.cassandraTable[Subject](settings("outputKeyspace"), settings("outputTable"))
    }

    /**
      * Saves the updated and new Subjects to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        updatedSubjects.saveToCassandra(settings("outputKeyspace"), settings("outputTable"))
    }
    // $COVERAGE-ON$

    /**
      * Updates the current Subjects with new Subjects of an already integrated datasource.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val version = Version(appName, List("datasourceupdate"), sc, false, settings.get("inputTable"))
        val updateAbleSubjects = subjects
            .filter(_.isSlave)
            .flatMap { subject =>
                val keys = subject.get(settings("datasourceKey"))
                keys.map(key => (key, subject))
            }
        updatedSubjects = subjectsWithUpdate
            .flatMap { subject =>
                val keys = subject.get(settings("datasourceKey"))
                keys.map(key => (key, subject))
            }.leftOuterJoin(updateAbleSubjects)
            .values
            .flatMap {
                case (newSubject, Some(oldSubject)) => List(updateSubject(oldSubject, newSubject, version))
                case (newSubject, None) => Merging.addToMasterNode(newSubject, version).map(_._1)
            }
    }
}

object DatasourceUpdate {
    /**
      * Updates the old Subject with the data of the new Subject.
      * @param oldSubject old Subject to be updated
      * @param newSubject Subject containing the new data with which the old Subject is updated
      * @param version Version used for versioning
      * @return the updated Subject
      */
    def updateSubject(oldSubject: Subject, newSubject: Subject, version: Version): Subject = {
        val sm = new SubjectManager(oldSubject, version)
        sm.setName(newSubject.name)
        sm.addAliases(newSubject.aliases)
        sm.setCategory(newSubject.category)
        sm.setProperties(newSubject.properties)
        sm.setRelations(newSubject.relations)
        oldSubject
    }
}
