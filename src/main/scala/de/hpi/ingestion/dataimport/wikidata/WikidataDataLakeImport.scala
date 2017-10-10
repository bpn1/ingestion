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

package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.dataimport.SharedNormalizations
import de.hpi.ingestion.dataimport.wikidata.models.WikidataEntity
import de.hpi.ingestion.datalake.{DataLakeImportImplementation, SubjectManager}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Translates each `WikidataEntity` into a `Subject` and writes it into a staging table.
  */
class WikidataDataLakeImport extends DataLakeImportImplementation[WikidataEntity](
	List("wikidata_20161117", "wikidata"),
	"wikidumps",
	"wikidata"
){
	appName = "WikidataDataLakeImport_v1.0"
	configFile = "datalake_import_wikidata.xml"
	importConfigFile = "normalization_wikidata.xml"
	val categoryMap = Map(
		"economic branch" -> "sector",
		"operation" -> "business",
		"exchange" -> "business",
		"financial institutions" -> "business",
		"institute" -> "business",
		"health maintenance organization" -> "business",
		"non-governmental organization" -> "business",
		"company" -> "business",
		"state" -> "country",
		"environmental organization" -> "business",
		"association" -> "business",
		"enterprise" -> "business",
		"medical organization" -> "business",
		"lobbying organization" -> "business",
		"collection agency" -> "business",
		"media company" -> "business",
		"chamber of commerce" -> "business",
		"copyright collective" -> "business",
		"business enterprise" -> "business",
		"local government in Germany" -> "city",
		"statutory corporation" -> "organization",
		"municipality of Germany" -> "city"
	)

	// $COVERAGE-OFF$
	/**
	  * Loads the Wikidata entities from the Cassandra.
	  * @param sc SparkContext to be used for the job
	  */
	override def load(sc: SparkContext): Unit = {
		inputEntities = sc.cassandraTable[WikidataEntity](inputKeyspace, inputTable)
	}
	// $COVERAGE-ON$

	override def filterEntities(entity: WikidataEntity): Boolean = {
		entity.instancetype.isDefined
	}

	override def normalizeAttribute(
		attribute: String,
		values: List[String],
		strategies: Map[String, List[String]]
	): List[String] = {
		val normalized = WikidataNormalizationStrategy(attribute)(values)
		if (attribute == "gen_sectors") normalized.flatMap(x => strategies.getOrElse(x, List(x))) else normalized
	}

	override def translateToSubject(
		entity: WikidataEntity,
		version: Version,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]],
		classifier: AClassifier[Tag]
	): Subject = {
		val subject = Subject.empty(datasource = "wikidata")
		val sm = new SubjectManager(subject, version)

		sm.setName(entity.label)
		sm.setCategory(entity.instancetype.flatMap(categoryMap.get))
		sm.addAliases(entity.aliases)
		sm.addProperties(entity.data)

		val legalForm = subject.name.flatMap(extractLegalForm(_, classifier)).toList
		val normalizedLegalForm = SharedNormalizations.normalizeLegalForm(legalForm)
		sm.addProperties(Map("gen_legal_form" -> normalizedLegalForm))

		val normalizedProperties = normalizeProperties(entity, mapping, strategies)
		sm.addProperties(normalizedProperties)

		subject
	}
}
