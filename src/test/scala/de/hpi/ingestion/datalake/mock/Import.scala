package de.hpi.ingestion.datalake.mock

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.datalake.DataLakeImportImplementation
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._

object Import extends DataLakeImportImplementation[Entity](
	List("TestSource"),
	"datalake/normalization.xml",
	"datalake/categorization.xml",
	"inputKeySpace",
	"inputTable"
){
	appName = "TestImport"

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		List(sc.parallelize(Seq(Subject()))).toAnyRDD()
	}

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
	): Subject = Subject(name = Option(entity.root_value))

	override def parseNormalizationConfig(path: String): Map[String, List[String]] = {
		val url = getClass.getResource(s"/$path")
		this.parseNormalizationConfig(url)
	}

	override def parseCategoryConfig(path: String): Map[String, List[String]] = {
		val url = getClass.getResource(s"/$path")
		super.parseCategoryConfig(url)
	}

	override def normalizeProperties(
		entity: Entity,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]]
	): Map[String, List[String]] = {
		super.normalizeProperties(entity, mapping, strategies)
	}
}
