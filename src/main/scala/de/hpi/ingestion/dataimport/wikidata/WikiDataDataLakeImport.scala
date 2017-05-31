package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.datalake.{DataLakeImportImplementation, SubjectManager}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * This job translates Wikidata entities into Subjects and writes them into a staging table.
  */
object WikiDataDataLakeImport extends DataLakeImportImplementation[WikiDataEntity](
	List("wikidata_20161117"),
	"normalization_wikidata.xml",
	"categorization_wikidata.xml",
	"wikidumps",
	"wikidata"
){
	appName = s"WikiDataDataLakeImport_v1.0"

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = sc.cassandraTable[WikiDataEntity](inputKeyspace, inputTable)
		List(wikidata).toAnyRDD()
	}
	// $COVERAGE-ON$

	override def filterEntities(entity: WikiDataEntity): Boolean = {
		entity.instancetype.isDefined
	}

	override def normalizeAttribute(
		attribute: String,
		values: List[String],
		strategies: Map[String, List[String]]
	): List[String] = {
		val normalized = WikiDataNormalizationStrategy(attribute)(values)
		if (attribute == "gen_sectors") normalized.flatMap(x => strategies.getOrElse(x, List(x))) else normalized
	}

	override def translateToSubject(
		entity: WikiDataEntity,
		version: Version,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]]
	): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		entity.label.foreach(label => sm.setName(label))
		entity.instancetype.foreach(instancetype => sm.setCategory(instancetype))
		if(entity.aliases.nonEmpty) sm.addAliases(entity.aliases)

		val normalizedProperties = normalizeProperties(entity, mapping, strategies)

		sm.addProperties(entity.data ++ normalizedProperties)
		subject
	}

}
