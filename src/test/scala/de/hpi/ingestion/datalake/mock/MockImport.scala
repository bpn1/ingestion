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

package de.hpi.ingestion.datalake.mock

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.datalake.DataLakeImportImplementation
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._

object MockImport extends DataLakeImportImplementation[Entity](
	List("TestSource"),
	"inputKeySpace",
	"inputTable"
){
	appName = "TestImport"
	importConfigFile = "src/test/resources/datalake/normalization.xml"

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = Nil

	override def filterEntities(entity: Entity): Boolean = true

	override def normalizeAttribute(
		attribute: String,
		values: List[String],
		strategies: Map[String, List[String]]
	): List[String] = values

	override def translateToSubject(
		entity: Entity,
		version: Version,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]],
		classifier: AClassifier[Tag]
	): Subject = Subject(master = null, datasource = null, name = Option(entity.root_value))

	override def normalizeProperties(
		entity: Entity,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]]
	): Map[String, List[String]] = {
		super.normalizeProperties(entity, mapping, strategies)
	}
}
