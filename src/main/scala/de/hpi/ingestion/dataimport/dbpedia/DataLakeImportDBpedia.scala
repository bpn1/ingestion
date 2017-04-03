package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.{DataLakeImport, SubjectManager}
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity
import scala.collection.mutable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

object DataLakeImportDBpedia extends DataLakeImport[DBPediaEntity](
	"DataLakeImportDBpedia_v1.0",
	List("dbpedia"),
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

		if(entity.label.isDefined) {
			sm.setName(entity.label.orNull)
		}

		val metadata = mutable.Map[String, List[String]]()
		metadata("dbpedianame") = List(entity.dbpedianame)
		if(entity.wikipageid.isDefined) {
			metadata("wikipageid") = List(entity.wikipageid.get)
		}
		if(entity.description.isDefined) {
			metadata("description") = List(entity.description.get)
		}
		if(entity.data.nonEmpty) {
			metadata ++= entity.data
		}
		sm.addProperties(metadata.toMap)
		subject
	}

	def main(args: Array[String]) {
		importToCassandra()
	}
}
