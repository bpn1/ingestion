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
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.RegexImplicits._

object KompassDataLakeImport extends DataLakeImportImplementation[KompassEntity](
	List("kompass", "kompass_20170206"),
	"datalake",
	"kompass_entities"
){
	appName = "KompassDataLakeImport_v1.0"
	configFile = "datalake_import_kompass.xml"
	importConfigFile = "normalization_kompass.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the Kompass entities from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val kompass = sc.cassandraTable[KompassEntity](inputKeyspace, inputTable)
		List(kompass).toAnyRDD()
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

	def extractAddress(address: String): Map[String, List[String]] = {
		address match {
			case r"""(.+)${street} (\d{5})${postal} (.+)${city} Deutschland""" =>
				Map(
					"geo_street" -> List(street.replaceFirst("str\\.", "straße")),
					"geo_postal" -> List(postal),
					"geo_city" -> List(city),
					"geo_country" -> List("DE")
				)
			case _ => Map()
		}
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

		sm.setCategory(entity.instancetype)
		sm.addProperties(entity.data)

		val addresses = subject
			.properties
			.collect { case (key, value) if key == "address" => extractAddress(value.head) }
		addresses.foreach(address => sm.addProperties(address))

		val legalForm = subject.name.flatMap(extractLegalForm(_, classifier)).toList
		val normalizedLegalForm = SharedNormalizations.normalizeLegalForm(legalForm)
		sm.addProperties(Map("gen_legal_form" -> normalizedLegalForm))

		val normalizedProperties = normalizeProperties(entity, mapping, strategies)
		sm.addProperties(normalizedProperties)

		if (normalizedProperties.contains("geo_coords_lat") && normalizedProperties.contains("geo_coords_long")) {
			val coords = normalizedProperties("geo_coords_lat")
				.zip(normalizedProperties("geo_coords_long"))
				.map { case (lat, long) => s"$lat;$long" }
			sm.addProperties(Map("geo_coords" -> coords))
		}

		subject
	}
}
