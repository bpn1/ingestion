package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
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
	List("wikidata_20161117", "wikidata"),
	"wikidumps",
	"wikidata"
){
	appName = "WikiDataDataLakeImport_v1.0"
	configFile = "datalake_import_wikidata.xml"
	importConfigFile = "normalization_wikidata.xml"
	val categoryMap = Map(
		"economic branch" -> "sector",
		"operation" -> "business",
		"exchange" -> "business",
		"financial institutions" -> "business",
		"institute" -> "business",
		"health maintenance organization" -> "business",
		"non-governmental organization" -> "business",
		"company" -> "business",
		"state" -> "business",
		"environmental organization" -> "business",
		"association" -> "business",
		"enterprise" -> "business",
		"medical organization" -> "business",
		"lobbying organization" -> "business",
		"collection agency" -> "business",
		"media company" -> "business",
		"chamber of commerce" -> "business",
		"copyright collective" -> "business",
		"business enterprise" -> "business"
	)

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
		strategies: Map[String, List[String]],
		classifier: AClassifier[Tag]
	): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		sm.setName(entity.label)
		sm.setCategory(entity.instancetype.flatMap(categoryMap.get))
		sm.addAliases(entity.aliases)

		val normalizedProperties = normalizeProperties(entity, mapping, strategies)
		sm.addProperties(entity.data ++ normalizedProperties)
		val legalForm = subject.name.map(extractLegalForm(_, classifier)).getOrElse(Nil)
		if (legalForm.nonEmpty) sm.addProperties(Map("gen_legal_form" -> legalForm))

		subject
	}
}
