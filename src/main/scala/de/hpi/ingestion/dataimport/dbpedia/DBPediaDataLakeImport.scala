package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.{DataLakeImport, SubjectManager}
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import scala.collection.mutable

/**
  * Import-Job to import DBPedia Subjects into the staging table of our datalake.
  */
object DBPediaDataLakeImport extends DataLakeImport[DBPediaEntity](
	"DataLakeImportDBpedia_v1.0",
	List("dbpedia"),
	Option("datalakeimport_config.xml"),
	"normalization_dbpedia.xml",
	"wikidumps",
	"dbpedia"
){
	override def readInput(sc: SparkContext, version: Version): RDD[Subject] = {
		val mapping = parseNormalizationConfig(this.normalizationFile)
		sc
			.cassandraTable[DBPediaEntity](inputKeyspace, inputTable)
			.filter(filterEntities)
			.map(translateToSubject(_, version, mapping))
	}

	override def filterEntities(entity: DBPediaEntity): Boolean = {
		entity.instancetype.isDefined
	}

	override def translateToSubject(
		entity: DBPediaEntity,
		version: Version,
		mapping: Map[String, List[String]]
	): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		entity.label.foreach(label => sm.setName(label.replaceAll("@de .$", "")))
		entity.instancetype.foreach(instancetype => sm.setCategory(instancetype))

		val normalizedProperties = normalizeProperties(entity, mapping)
		val properties = mutable.Map[String, List[String]]()
		properties ++= (entity.data ++ normalizedProperties)

		if (normalizedProperties.contains("geo_coords_lat") && normalizedProperties.contains("geo_coords_long")) {
			properties("geo_coords") = normalizedProperties("geo_coords_lat")
				.zip(normalizedProperties("geo_coords_long"))
				.flatMap(x => List(x._1, x._2))
		}

		sm.addProperties(properties.toMap)
		subject
	}

	def main(args: Array[String]) {
		importToCassandra(args.headOption)
	}
}
