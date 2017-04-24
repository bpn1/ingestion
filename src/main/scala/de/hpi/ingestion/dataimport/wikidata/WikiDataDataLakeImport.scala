package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.datalake.{DataLakeImport, SubjectManager}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * This job translates Wikidata entities into Subjects and writes them into a staging table.
  */
object WikiDataDataLakeImport extends DataLakeImport[WikiDataEntity](
	"WikiDataDataLakeImport_v1.0",
	List("wikidata_20161117"),
	"normalization_wikidata.xml",
	"wikidumps",
	"wikidata")
{
	override def readInput(sc: SparkContext, version: Version): RDD[Subject] = {
		sc
			.cassandraTable[WikiDataEntity](inputKeyspace, inputTable)
			.filter(filterEntities)
			.map(translateToSubject(_, version))
	}

	override def filterEntities(entity: WikiDataEntity): Boolean = {
		entity.instancetype.isDefined
	}

	override def translateToSubject(entity: WikiDataEntity, version: Version): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		entity.label.foreach(label => sm.setName(label))
		entity.instancetype.foreach(instancetype => sm.setCategory(instancetype))
		if(entity.aliases.nonEmpty) sm.addAliases(entity.aliases)

		val mapping = parseNormalizationConfig(this.normalizationFile)
		val normalizedProperties = normalizeProperties(entity, mapping)

		sm.addProperties(entity.data ++ normalizedProperties)
		subject
	}

	def main(args: Array[String]): Unit = {
		importToCassandra()
	}

}
