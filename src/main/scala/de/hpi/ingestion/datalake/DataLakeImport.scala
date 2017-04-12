package de.hpi.ingestion.datalake

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.util.Date
import org.apache.spark.rdd.RDD
import com.datastax.driver.core.utils.UUIDs
import de.hpi.ingestion.datalake.models.{Subject, Version}

trait DLImport[T] extends Serializable {
	val outputKeyspace = "datalake"
	val outputTable = "subject_temp"
	val versionTable = "version"

	/**
	  * Reads the data of the datasoure to import and transforms the entries to Subjects
	  *
	  * @param sc      the SparkContext of the program
	  * @param version the version of the new Subjects
	  * @return a RDD of new Subjects created from the given objects
	  */
	protected def readInput(sc: SparkContext, version: Version): RDD[Subject]

	/**
	  * Translates an object to the schema of the staging table
	  * @param entity  the object to be converted to a Subject
	  * @param version the version of the new Subject
	  * @return a new Subject created from the given object
	  */
	protected def translateToSubject(entity: T, version: Version): Subject

	/**
	  * A generic import from the new data table to the staging area table
	  */
	protected def importToCassandra(): Unit
}

/**
  * An abstract DataLakeImport to import new sources to the staging table
  *
  * @constructor create a new DataLakeImport with an appName, dataSources, an inputKeyspace
  *              and an inputTable
  * @param appName       the name of the importing job
  * @param dataSources   list of the sources where the new data is fetched from
  * @param inputKeyspace the name of the keyspace where the new data is saved in the database
  * @param inputTable    the name of the table where the new data is saved in the database
  * @tparam T the type of Objects of the new data
  */
abstract case class DataLakeImport[T](
	appName: String,
	dataSources: List[String],
	inputKeyspace: String,
	inputTable: String
) extends DLImport[T] {

	protected def importToCassandra(): Unit = {
		val conf = new SparkConf()
			.setAppName(appName)
		val sc = new SparkContext(conf)

		val version = Version(appName, dataSources, sc)
		val data = readInput(sc, version)
		data.saveToCassandra(outputKeyspace, outputTable)

		sc.stop
	}
}
