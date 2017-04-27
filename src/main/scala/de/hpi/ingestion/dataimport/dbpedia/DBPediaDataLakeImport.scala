package de.hpi.ingestion.dataimport.dbpedia

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.{DataLakeImport, SubjectManager}
import de.hpi.ingestion.dataimport.dbpedia.models.DBpediaEntity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.collection.mutable

/**
  * Import-Job to import DBpedia Subjects into the staging table of our datalake.
  */
object DBpediaDataLakeImport extends DataLakeImport[DBpediaEntity](
	List("dbpedia"),
	Option("datalakeimport_config.xml"),
	"normalization_dbpedia.xml",
	"wikidumps",
	"dbpedia"
){
	appName = s"DataLakeImportDBpedia_v1.0_${System.currentTimeMillis()}"

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

	override def translateToSubject(
		entity: DBpediaEntity,
		version: Version,
		mapping: Map[String, List[String]]
	): Subject = {
		val subject = Subject()
		val sm = new SubjectManager(subject, version)

		entity.label.foreach(label => sm.setName(label.replaceAll("@de .$", "")))
		entity.instancetype.foreach(instancetype => sm.setCategory(instancetype))

		val normalizedProperties = normalizeProperties(entity, mapping)
		val properties = mutable.Map[String, List[String]]()
		properties ++= (entity.data ++ normalizedProperties)

		if (normalizedProperties.contains("geo_coords_lat") && normalizedProperties.contains("geo_coords_long")) {
			properties("geo_coords") = normalizedProperties("geo_coords_lat")
				.zip(normalizedProperties("geo_coords_long"))
				.flatMap(x => List(x._1, x._2))
		}

		sm.addProperties(properties.toMap)
		subject
	}
}
