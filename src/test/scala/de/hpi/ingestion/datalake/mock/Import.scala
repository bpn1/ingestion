package de.hpi.ingestion.datalake.mock

import de.hpi.ingestion.datalake.DataLakeImport
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._

object Import extends DataLakeImport[Entity](
	List("TestSource"),
	Option("datalakeimport.xml"),
	"normalization.xml",
	"inputKeySpace",
	"inputTable"
){
	appName = "TestImport"

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		List(sc.parallelize(Seq(Subject()))).toAnyRDD()
	}

	override def filterEntities(entity: Entity): Boolean = true
	override def translateToSubject(
		entity: Entity,
		version: Version,
		mapping: Map[String, List[String]]
	): Subject = Subject()
	override def parseConfig(path: String): Unit = super.parseConfig(path)
	override def parseNormalizationConfig(path: String): Map[String, List[String]] = {
		val url = getClass.getResource(s"/$path")
		super.parseNormalizationConfig(url)
	}
	override def normalizeProperties(entity: Entity, mapping: Map[String, List[String]]): Map[String, List[String]] = {
		super.normalizeProperties(entity, mapping)
	}
}
