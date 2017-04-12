package de.hpi.ingestion.datalake.models

import java.util.{Date, UUID}

import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._
import org.apache.spark.SparkContext

case class Version(
	version: UUID = UUIDs.timeBased(),
	var program: String,
	var value: List[String] = List[String](),
	var validity: Map[String, String] = Map[String, String](),
	var datasources: List[String] = List[String](),
	var timestamp: Date = new Date()
)

object Version {
	val keyspace = "datalake"
	val tablename = "version"

	def apply(program: String, datasources: List[String], sc: SparkContext): Version = {
		val version = Version(program = program, datasources = datasources)
		if(sc.getConf.contains("spark.cassandra.connection.host")) {
			val logVersion = (version.version, version.timestamp, version.datasources, version.program)
			sc.parallelize(List(logVersion))
				.saveToCassandra(keyspace, tablename, SomeColumns("version", "timestamp", "datasources", "program"))
		}
		version
	}
}
