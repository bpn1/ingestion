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

import java.io.{ByteArrayOutputStream, PrintStream}

import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.github.powerlibraries.io.In
import de.hpi.ingestion.datalake.models.{DLImportEntity, Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.companies.algo.Tag

/**
  * An abstract DataLakeImportImplementation to import new sources to the staging table.
  *
  * @constructor Create a new DataLakeImportImplementation with an appName, dataSources, an inputKeyspace
  *              and an inputTable.
  * @param dataSources       list of the sources where the new data is fetched from
  * @param inputKeyspace     the name of the keyspace where the new data is saved in the database
  * @param inputTable        the name of the table where the new data is saved in the database
  * @tparam T the type of Objects of the new data
  */
abstract case class DataLakeImportImplementation[T <: DLImportEntity](
    dataSources: List[String],
    inputKeyspace: String,
    inputTable: String
) extends DataLakeImport[T] with SparkJob {
    configFile = "datalake_import.xml"

    var inputEntities: RDD[T] = _
    var subjects: RDD[Subject] = _

    // $COVERAGE-OFF$
    /**
      * Writes the Subjects to the outputTable table in keyspace outputKeyspace.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        subjects.saveToCassandra(settings("outputKeyspace"), settings("outputTable"))
    }
    // $COVERAGE-ON$

    /**
      * Filters the input entities and then transforms them to Subjects.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val version = Version(appName, dataSources, sc, false, settings.get("outputTable"))
        val mapping = sc.broadcast(normalizationSettings)
        val strategies = sc.broadcast(sectorSettings)
        val classifier = this.classifier
        subjects = inputEntities
            .filter(filterEntities)
            .map((entity: T) => translateToSubject(entity, version, mapping.value, strategies.value, classifier))
    }

    override def filterEntities(entity: T): Boolean = true

    override def normalizeProperties(
        entity: T,
        mapping: Map[String, List[String]],
        strategies: Map[String, List[String]]
    ): Map[String, List[String]] = {
        mapping
            .map { case (normalized, notNormalized) =>
                val values = notNormalized.flatMap(entity.get)
                val normalizedValues = this.normalizeAttribute(normalized, values, strategies)
                (normalized, normalizedValues)
            }.filter(_._2.nonEmpty)
    }

    override def extractLegalForm(name: String, classifier: AClassifier[Tag]): Option[String] = {
        val tags = List(Tag.LEGAL_FORM)
        try {
            val legalForm = classifier
                .getTags(name)
                .filter(pair => tags.contains(pair.getValue))
                .map(_.getKey.getRawForm)
            Option(legalForm).filter(_.nonEmpty).map(_.mkString(" "))
        } catch {
            case _: Throwable => None
        }
    }

    override def classifier: AClassifier[Tag] = {
        val stdout = System.out
        System.setOut(new PrintStream(new ByteArrayOutputStream()))
        val fileStream = getClass.getResource("/bin/StanfordCRFClassifier-Tag.bin").openStream()
        val classifier = In.stream(fileStream).readObject[AClassifier[Tag]]()
        System.setOut(stdout)
        classifier
    }
}
