package de.hpi.ingestion.datalake

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import java.net.URL
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.datalake.models.{DLImportEntity, Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import scala.io.Source
import scala.xml.XML

/**
  * An abstract DataLakeImportImplementation to import new sources to the staging table.
  *
  * @constructor Create a new DataLakeImportImplementation with an appName, dataSources, an inputKeyspace
  *              and an inputTable.
  * @param dataSources       list of the sources where the new data is fetched from
  * @param inputKeyspace     the name of the keyspace where the new data is saved in the database
  * @param inputTable        the name of the table where the new data is saved in the database
  * @tparam T the type of Objects of the new data
  */
abstract case class DataLakeImportImplementation[T <: DLImportEntity](
	dataSources: List[String],
	inputKeyspace: String,
	inputTable: String
) extends DataLakeImport[T] with SparkJob {
	configFile = "datalake_import.xml"

	// $COVERAGE-OFF$
	/**
	  * Writes the Subjects to the {@outputTable} table in keyspace {@outputKeyspace}.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(settings("outputKeyspace"), settings("outputTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Filters the input entities and then transforms them to Subjects.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val version = Version(appName, dataSources, sc, true)
		val mapping = sc.broadcast(normalizationSettings)
		val strategies = sc.broadcast(sectorSettings)
		input
			.fromAnyRDD[T]()
			.map(rdd =>
				rdd
					.filter(filterEntities)
					.map((entity: T) => translateToSubject(entity, version, mapping.value, strategies.value)))
			.toAnyRDD()
	}

	override protected def filterEntities(entity: T): Boolean = true

	protected def normalizeProperties(
		entity: T,
		mapping: Map[String, List[String]],
		strategies: Map[String, List[String]]
	): Map[String, List[String]] = {
		mapping
			.map { case (normalized, notNormalized) =>
				val values = notNormalized.flatMap(entity.get)
				val normalizedValues = this.normalizeAttribute(normalized, values, strategies)
				(normalized, normalizedValues)
			}.filter(_._2.nonEmpty)
	}
}
