package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.datalake.{DataLakeImport, SubjectManager}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * This job translates Wikidata entities into Subjects and writes them into a staging table.
  */
object WikiDataDataLakeImport extends DataLakeImport[WikiDataEntity](
	List("wikidata_20161117"),
	Option("datalakeimport_config.xml"),
	"normalization_wikidata.xml",
	"wikidumps",
	"wikidata"
){
	appName = s"WikiDataDataLakeImport_v1.0_${System.currentTimeMillis()}"

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = sc.cassandraTable[WikiDataEntity](inputKeyspace, inputTable)
		List(wikidata).toAnyRDD()
	}
	// $COVERAGE-ON$

	override def filterEntities(entity: WikiDataEntity): Boolean = {
		entity.instancetype.isDefined
	}

	override def translateToSubject(
		entity: WikiDataEntity,
		version: Version,
		mapping: Map[String, List[String]]
	): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		entity.label.foreach(label => sm.setName(label))
		entity.instancetype.foreach(instancetype => sm.setCategory(instancetype))
		if(entity.aliases.nonEmpty) sm.addAliases(entity.aliases)

		val normalizedProperties = normalizeProperties(entity, mapping)

		sm.addProperties(entity.data ++ normalizedProperties)
		subject
	}

}
