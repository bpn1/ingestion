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

package de.hpi.ingestion.dataimport.kompass

import de.hpi.ingestion.dataimport.kompass.models.KompassEntity
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.datalake.{DataLakeImportImplementation, SubjectManager}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.dataimport.SharedNormalizations
import de.hpi.ingestion.implicits.RegexImplicits._

class KompassDataLakeImport extends DataLakeImportImplementation[KompassEntity](
    List("kompass", "kompass_20170206"),
    "datalake",
    "kompass_entities"
){
    configFile = "datalake_import_kompass.xml"
    importConfigFile = "normalization_kompass.xml"
    appName = "KompassDataLakeImport_v1.0"

    // $COVERAGE-OFF$
    /**
      * Loads the Kompass entities from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        inputEntities = sc.cassandraTable[KompassEntity](inputKeyspace, inputTable)
    }
    // $COVERAGE-ON$

    override def normalizeAttribute(
        attribute: String,
        values: List[String],
        strategies: Map[String, List[String]]
    ): List[String] = {
        val normalized = KompassNormalizationStrategy(attribute)(values)
        if(attribute == "gen_sectors") normalized.flatMap(x => strategies.getOrElse(x, List(x))) else normalized
    }

    override def translateToSubject(
        entity: KompassEntity,
        version: Version,
        mapping: Map[String, List[String]],
        strategies: Map[String, List[String]],
        classifier: AClassifier[Tag]
    ): Subject = {
        val subject = Subject.empty(datasource = "kompass")
        val sm = new SubjectManager(subject, version)

        sm.setName(entity.name)
        sm.setCategory(entity.instancetype)
        sm.addProperties(entity.data)

        val legalForm = subject.name.flatMap(extractLegalForm(_, classifier)).toList
        val normalizedLegalForm = SharedNormalizations.normalizeLegalForm(legalForm)
        sm.addProperties(Map("gen_legal_form" -> normalizedLegalForm))

        val normalizedProperties = normalizeProperties(entity, mapping, strategies)
        sm.addProperties(normalizedProperties)

        subject
    }
}
