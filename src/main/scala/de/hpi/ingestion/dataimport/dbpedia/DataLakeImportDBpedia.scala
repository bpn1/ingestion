package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.{DataLakeImport, SubjectManager}
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

import scala.collection.mutable

object DataLakeImportDBpedia extends DataLakeImport[DBPediaEntity](
	"DataLakeImportDBpedia_v1.0",
	List("dbpedia"),
	"normalize_dbpedia.xml",
	"wikidumps",
	"dbpedia"
){
	override def readInput(sc: SparkContext, version: Version): RDD[Subject] = {
		sc
			.cassandraTable[DBPediaEntity](inputKeyspace, inputTable)
			.map(translateToSubject(_, version))
	}

	override def translateToSubject(entity: DBPediaEntity, version: Version): Subject = {
		val mapping = parseNormalizationConfig(this.normalizationFile)
		val normalizedProperties = normalizeProperties(entity, mapping)
		val properties = mutable.Map[String, List[String]]()
		properties ++= (entity.data ++ normalizedProperties)

		properties += ("geo_coords" -> List(
			normalizedProperties("geo_coords_lat").head,
			normalizedProperties("geo_coords_long").head
		))

		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		if(entity.label.isDefined) {
			sm.setName(entity.label.orNull)
		}

		sm.addProperties(entity.data ++ properties)
		subject
	}

	def main(args: Array[String]) {
		importToCassandra()
	}
}
