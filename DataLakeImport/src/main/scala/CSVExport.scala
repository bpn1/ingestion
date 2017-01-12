package DataLake

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

object CSVExport {
	val appname = "CSVExport_v1.0"

	val keyspace = "datalake"
	val tablename = "subject"

	val quote = "\""
	val separator = ","
	val arraySeparator = ";"

	// header:
	// :ID(Subject),name,aliases:string[],category:LABEL
	def nodeToCSV(subject: Subject): String = {
		val aliasString = subject.aliases
			.mkString(arraySeparator)
			.replace("\\", "")
			.replace("\"", "\\\"")
		val name = subject.name.getOrElse("").replace("\"", "'")
		var output = List(subject.id.toString, name, aliasString, subject.category.getOrElse("")).mkString(quote+separator+quote)

		// TODO serialize properties to JSON string
		quote + output + quote
	}

	// header:
	// :START_ID(Subject),:END_ID(Subject),:TYPE
	def edgesToCSV(subject: Subject): String = {
		val lines = subject.relations.map { case (id, props) =>
			val relType = props.getOrElse("type", "")
			subject.id + separator + id + separator + relType
		}

		lines.mkString("\n").trim
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName(appname)
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		val subjects = sc.cassandraTable[Subject](keyspace, tablename)

		val timestamp = System.currentTimeMillis / 1000

		val nodes = subjects
			.map(nodeToCSV)
			.saveAsTextFile("export_nodes_" + timestamp)

		val edges = subjects
			.map(edgesToCSV)
			.filter(_.trim != "")
			.saveAsTextFile("export_edges_" + timestamp)
	}
}
