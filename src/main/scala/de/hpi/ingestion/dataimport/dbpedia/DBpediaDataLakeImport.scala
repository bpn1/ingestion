package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.{DataLakeImportImplementation, SubjectManager}
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.companies.algo.Tag
import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.collection.mutable

/**
  * Import-Job to import DBpedia Subjects into the staging table of the datalake.
  */
object DBpediaDataLakeImport extends DataLakeImportImplementation[DBpediaEntity](
	List("dbpedia", "dbpedia_20161203"),
	"wikidumps",
	"dbpedia"
){
	appName = "DBpediaDataLakeImport_v1.0"
	importConfigFile = "normalization_dbpedia.xml"

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
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		entity.label.foreach(label => sm.setName(label.replaceAll("@de .$", "")))
		entity.instancetype.foreach(instancetype => sm.setCategory(instancetype))

		val normalizedProperties = normalizeProperties(entity, mapping, strategies)
		val properties = mutable.Map[String, List[String]]((entity.data ++ normalizedProperties).toSeq: _*)

		if (normalizedProperties.contains("geo_coords_lat") && normalizedProperties.contains("geo_coords_long")) {
			properties("geo_coords") = normalizedProperties("geo_coords_lat")
				.zip(normalizedProperties("geo_coords_long"))
				.map { case (lat, long) => s"$lat;$long" }
		}

		val legalForm = entity.label.map(extractLegalForm(_, classifier)).getOrElse(Nil)
		if (legalForm.nonEmpty) properties("gen_legal_form") = legalForm

		sm.addProperties(properties.toMap)
		subject
	}
}
