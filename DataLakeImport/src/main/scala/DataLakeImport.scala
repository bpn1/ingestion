package DataLake

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.util.Date

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import scala.reflect.ClassTag

trait DLImport[T] extends Serializable {
	val outputKeyspace = "datalake"
	val outputTable = "subject_temp"
	val versionTable = "version"
	protected def translateToSubject(entity: T, version: Version): Subject
	protected def makeTemplateVersion(): Version
	protected def importToCassandra(): Unit
}

abstract case class DataLakeImport[T <: Serializable : ClassTag : RowReaderFactory](
	appName: String,
	dataSources: List[String],
	inputKeyspace: String,
	inputTable: String
) extends DLImport[T] with Serializable {
	protected def makeTemplateVersion(): Version = {
		// create timestamp and TimeUUID for versioning
		val timestamp = new Date()
		val version = UUIDs.timeBased()

		Version(version, appName, null, null, dataSources, timestamp)
	}

	protected def importToCassandra(): Unit = {
		val conf = new SparkConf()
			.setAppName(appName)
			.set("spark.cassandra.connection.host", "odin01")
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
