package de.hpi.ingestion.datalake

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.net.URL

import org.apache.spark.rdd.RDD
import de.hpi.ingestion.datalake.models.{DLImportEntity, Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.io.Source
import scala.xml.XML

/**
  * An abstract DataLakeImport to import new sources to the staging table
  *
  * @constructor create a new DataLakeImport with an appName, dataSources, an inputKeyspace
  *              and an inputTable
  * @param appName       		the name of the importing job
  * @param dataSources   		list of the sources where the new data is fetched from
  * @param normalizationFile 	name of normalization file in resource folder
  * @param inputKeyspace 		the name of the keyspace where the new data is saved in the database
  * @param inputTable    		the name of the table where the new data is saved in the database
  * @tparam T the type of Objects of the new data
  */
abstract case class DataLakeImport[T <: DLImportEntity](
	override var appName: String,
	dataSources: List[String],
	normalizationFile: String,
	inputKeyspace: String,
	inputTable: String
) extends DLImport[T] with SparkJob {
	protected def parseNormalizationConfig(url: URL): Map[String, List[String]] = {
		val xml = XML.loadString(Source
			.fromURL(url)
			.getLines()
			.mkString("\n"))

		(xml \\ "normalization" \ "attributeMapping" \ "attribute")
			.toList
			.map { attribute =>
				val key = (attribute \ "key").text
				val values = (attribute \ "mapping")
					.toList
					.map(_.text)
				(key, values)
			}.toMap
	}

	protected def parseNormalizationConfig(path: String): Map[String, List[String]] = {
		val url = getClass.getResource(s"/$path")
		this.parseNormalizationConfig(url)
	}

	protected def normalizeProperties(
		entity: T,
		mapping: Map[String, List[String]]
	): Map[String, List[String]] = {
		mapping
		    .mapValues(_.map(value => entity.get(value)).reduce(_ ::: _).distinct)
			.filter { case (key, values) => values.nonEmpty }
	}

	/**
	  * Filters the input entities and then transforms them to Subjects.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val version = Version(appName, dataSources, sc)
		input
			.fromAnyRDD[T]()
			.map(rdd =>
				rdd
					.filter(filterEntities)
					.map(translateToSubject(_, version)))
			.toAnyRDD()
	}

	/**
	  * Writes the Subjects to the {@outputTable} table in keyspace {@outputKeyspace}.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(outputKeyspace, outputTable)
	}
}
