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
object DataLakeImportDBpedia extends DataLakeImport[DBPediaEntity](
	"DataLakeImportDBpedia_v1.0",
	List("dbpedia"),
	"normalization_dbpedia.xml",
	"wikidumps",
	"dbpedia"
){
	override def readInput(sc: SparkContext, version: Version): RDD[Subject] = {
		sc
			.cassandraTable[DBPediaEntity](inputKeyspace, inputTable)
			.map(translateToSubject(_, version))
	}

	override def translateToSubject(entity: DBPediaEntity, version: Version): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		entity.label.foreach(label => sm.setName(label.replaceAll("@de .$", "")))

		val mapping = parseNormalizationConfig(this.normalizationFile)
		val normalizedProperties = normalizeProperties(entity, mapping)
		val properties = mutable.Map[String, List[String]]()
		properties ++= (entity.data ++ normalizedProperties)
		properties("geo_coords") = normalizedProperties("geo_coords_lat")
			.zip(normalizedProperties("geo_coords_long"))
		    .flatMap(x => List(x._1, x._2))

		sm.addProperties(entity.data ++ properties)
		subject
	}

	def main(args: Array[String]) {
		importToCassandra()
	}
}
