package de.hpi.ingestion.dataimport.kompass

import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.dataimport.SharedNormalizations
import de.hpi.ingestion.dataimport.dbpedia.DBpediaDataLakeImport._
import de.hpi.ingestion.dataimport.dbpedia.DBpediaNormalizationStrategy
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import de.hpi.ingestion.dataimport.kompass.models.KompassEntity
import de.hpi.ingestion.datalake.{DataLakeImportImplementation, SubjectManager}
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

object KompassDataLakeImport extends DataLakeImportImplementation[KompassEntity](
	List("kompass", "kompass_20170206"),
	"datalake",
	"kompass_entities"
){
	appName = "KompassDataLakeImport_v1.0"
	configFile = "datalake_import_kompass.xml"
	importConfigFile = "normalization_kompass.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the Kompass entities from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val kompass = sc.cassandraTable[KompassEntity](inputKeyspace, inputTable)
		List(kompass).toAnyRDD()
	}
	// $COVERAGE-ON$

	override def filterEntities(entity: KompassEntity): Boolean = {
		true
	}

	override def normalizeAttribute(
		attribute: String,
		values: List[String],
		strategies: Map[String, List[String]]
	): List[String] = {
		val normalized = KompassNormalizationStrategy(attribute)(values)
		if(attribute == "gen_sectors") normalized.flatMap(x => strategies.getOrElse(x, List(x))) else normalized
	}

	override def translateToSubject(
		entity: KompassEntity,
		version: Version,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]],
		classifier: AClassifier[Tag]
	): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		sm.setCategory(entity.instancetype)
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
