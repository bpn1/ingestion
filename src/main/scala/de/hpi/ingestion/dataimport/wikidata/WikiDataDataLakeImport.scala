package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.datalake.{DataLakeImport, SubjectManager}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object WikiDataDataLakeImport extends DataLakeImport[WikiDataEntity](
	"WikiDataDataLakeImport_v1.0",
	List("wikidata_20161117"),
	"wikidumps",
	"wikidata")
{
	override def readInput(sc: SparkContext, version: Version): RDD[Subject] = {
		sc
			.cassandraTable[WikiDataEntity](inputKeyspace, inputTable)
			.map(translateToSubject(_, version))
	}

	override def translateToSubject(entity: WikiDataEntity, version: Version): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		if(entity.label.isDefined) {
			sm.setName(entity.label.get)
		}
		if(entity.aliases.nonEmpty) {
			sm.addAliases(entity.aliases)
		}
		if(entity.instancetype.isDefined) {
			sm.setCategory(entity.instancetype.get)
		}
		val metadata = mutable.Map(("wikidata_id", List(entity.id)))
		if(entity.wikiname.isDefined) {
			metadata += "wikipedia_name" -> List(entity.wikiname.get)
		}
		if(entity.data.nonEmpty) {
			metadata ++= entity.data
		}
		sm.addProperties(metadata.toMap)
		subject
	}

	def main(args: Array[String]): Unit = {
		importToCassandra()
	}

}
