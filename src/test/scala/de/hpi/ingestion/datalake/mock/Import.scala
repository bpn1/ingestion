package de.hpi.ingestion.datalake.mock

import de.hpi.ingestion.datalake.DataLakeImport
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Import extends DataLakeImport[Entity](
	"TestImport",
	List("TestSource"),
	"normalization.xml",
	"inputKeySpace",
	"inputTable"
){
	override def readInput(sc: SparkContext, version: Version): RDD[Subject] = sc.parallelize(Seq(Subject()))
	override def filterEntities(entity: Entity): Boolean = true
	override def translateToSubject(entity: Entity, version: Version): Subject = Subject()
	override def parseNormalizationConfig(path: String): Map[String, List[String]] = {
		val url = getClass.getResource(s"/$path")
		super.parseNormalizationConfig(url)
	}
	override def normalizeProperties(entity: Entity, mapping: Map[String, List[String]]): Map[String, List[String]] = {
		super.normalizeProperties(entity, mapping)
	}
}
