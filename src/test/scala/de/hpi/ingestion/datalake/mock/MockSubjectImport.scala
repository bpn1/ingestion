package de.hpi.ingestion.datalake.mock

import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.datalake.DataLakeImportImplementation
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MockSubjectImport extends DataLakeImportImplementation[Entity](
	List("TestSource"),
	"inputKeySpace",
	"inputTable"
)  {
	appName = "TestImport"
	importConfigFile = "src/test/resources/datalake/normalization.xml"

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		Nil
	}

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
	): Subject = {
		Subject(id = null, name = Option(entity.root_value), properties = entity.data)
	}
}
