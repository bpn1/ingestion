package DataLake

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.util.Date

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.rdd.reader.RowReaderFactory

import scala.reflect.ClassTag

/**
	* An abstract DataLakeImport to import new sources to the staging table
	* @constructor create a new DataLakeImport with an appName, dataSources, an inputKeyspace and an inputTable
	* @param appName the name of the importing job
	* @param dataSources list of the sources where the new data is fetched from
	* @param inputKeyspace the name of the keyspace where the new data is saved in the database
	* @param inputTable the name of the table where the new data is saved in the database
	* @tparam T the type of Objects of the new data
	*/
abstract class DataLakeImport[T <: Serializable : ClassTag : RowReaderFactory](
	val appName: String,
	val dataSources: List[String],
	val inputKeyspace: String,
	val inputTable: String
){
	val outputKeyspace = "datalake"
	val outputTable = "staging"
	val versionTable = "version"

	/**
		* Translates an object to the schema of the staging table
		* @param entity the object to be converted to a Subject
		* @param version the version of the new Subject
		* @return a new Subject created from the given object
		*/
	protected def translateToSubject(entity: T, version: Version): Subject

	/**
		* Createds a new version for the import
		* @return a version reproducing to the details of the import
		*/
	protected def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	/**
		* A generic import from the new data table to the staging area table
		*/
	protected def importToCassandra(): Unit = {
		val conf = new SparkConf()
			.setAppName(appName)
			.set("spark.cassandra.connection.host", "172.20.21.11")
		val sc = new SparkContext(conf)

		val data = sc.cassandraTable[T](inputKeyspace, inputTable)
		val version = makeTemplateVersion()

		data
		  .map(translateToSubject(_, version))
		  .saveToCassandra(outputKeyspace, outputTable)

		sc
			.parallelize(List((version.version, version.timestamp, version.datasources, version.program)))
			.saveToCassandra(outputKeyspace, versionTable, SomeColumns("version", "timestamp", "datasources", "program"))

		sc.stop
	}
}