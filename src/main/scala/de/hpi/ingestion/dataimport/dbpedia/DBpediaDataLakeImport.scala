package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.{DataLakeImportImplementation, SubjectManager}
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.dataimport.SharedNormalizations
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Import-Job to import DBpedia Subjects into the staging table of the datalake.
  */
object DBpediaDataLakeImport extends DataLakeImportImplementation[DBpediaEntity](
	List("dbpedia", "dbpedia_20161203"),
	"wikidumps",
	"dbpedia"
){
	appName = "DBpediaDataLakeImport_v1.0"
	configFile = "datalake_import_dbpedia.xml"
	importConfigFile = "normalization_dbpedia.xml"
	val categoryMap = Map(
		"Broadcaster" -> "business",
		"Company" -> "business",
		"EducationalInstitution" -> "organization",
		"EmployersOrganisation" -> "business",
		"GeopoliticalOrganisation" -> "organization",
		"GovernmentAgency" -> "business",
		"InternationalOrganisation" -> "business",
		"Legislature" -> "organization",
		"MilitaryUnit" -> "organization",
		"Non-ProfitOrganisation" -> "organization",
		"Parliament" -> "organization",
		"PoliticalParty" -> "organization",
		"PublicTransitSystem" -> "business",
		"ReligiousOrganisation" -> "organization",
		"SambaSchool" -> "business",
		"SportsClub" -> "business",
		"SportsLeague" -> "business",
		"SportsTeam" -> "business",
		"TermOfOffice" -> "organization",
		"TradeUnion" -> "business"
	)

	// $COVERAGE-OFF$
	/**
	  * Loads the DBpedia entities from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = sc.cassandraTable[DBpediaEntity](inputKeyspace, inputTable)
		List(wikidata).toAnyRDD()
	}
	// $COVERAGE-ON$

	override def filterEntities(entity: DBpediaEntity): Boolean = {
		entity.instancetype.isDefined
	}

	override def normalizeAttribute(
		attribute: String,
		values: List[String],
		strategies: Map[String, List[String]]
	): List[String] = {
		val normalized = DBpediaNormalizationStrategy(attribute)(values)
		if(attribute == "gen_sectors") normalized.flatMap(x => strategies.getOrElse(x, List(x))) else normalized
	}

	override def translateToSubject(
		entity: DBpediaEntity,
		version: Version,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]],
		classifier: AClassifier[Tag]
	): Subject = {
		val subject = Subject.empty(datasource = "dbpedia")
		val sm = new SubjectManager(subject, version)

		sm.setName(entity.label.map(_.replaceAll("""@de \.$""", "")))
		sm.setCategory(entity.instancetype.flatMap(categoryMap.get))
		sm.addProperties(entity.data)

		val legalForm = subject.name.flatMap(extractLegalForm(_, classifier)).toList
		val normalizedLegalForm = SharedNormalizations.normalizeLegalForm(legalForm)
		sm.addProperties(Map("gen_legal_form" -> normalizedLegalForm))

		val normalizedProperties = normalizeProperties(entity, mapping, strategies)
		sm.addProperties(normalizedProperties)

		if (normalizedProperties.contains("geo_coords_lat") && normalizedProperties.contains("geo_coords_long")) {
			val coords = normalizedProperties("geo_coords_lat")
				.zip(normalizedProperties("geo_coords_long"))
				.map { case (lat, long) => s"$lat;$long" }
			sm.addProperties(Map("geo_coords" -> coords))
		}

		subject
	}
}
