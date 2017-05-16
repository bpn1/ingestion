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
  * @param configFile        name of config file in resource folder
  * @param normalizationFile name of normalization file in resource folder
  * @param inputKeyspace     the name of the keyspace where the new data is saved in the database
  * @param inputTable        the name of the table where the new data is saved in the database
  * @tparam T the type of Objects of the new data
  */
abstract case class DataLakeImportImplementation[T <: DLImportEntity](
	dataSources: List[String],
	configFile: Option[String],
	normalizationFile: String,
	inputKeyspace: String,
	inputTable: String
) extends DataLakeImport[T] with SparkJob {

	// $COVERAGE-OFF$
	/**
	  * Writes the Subjects to the {@outputTable } table in keyspace {@outputKeyspace }.
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

	override protected def filterEntities(entity: T): Boolean = true

	protected def parseConfig(url: URL): Unit = {
		val xml = XML.loadString(Source
			.fromURL(url)
			.getLines()
			.mkString("\n")
		)

		val configSettings = xml \\ "config" \ "settings"
		for(node <- configSettings.head.child if node.text.trim.nonEmpty)
			settings(node.label) = node.text
	}

	protected def parseConfig(path: String): Unit = {
		val url = getClass.getResource(s"/$path")
		this.parseConfig(url)
	}

	protected def parseNormalizationConfig(url: URL): Map[String, List[String]] = {
		val xml = XML.loadString(Source
			.fromURL(url)
			.getLines()
			.mkString("\n"))

		(xml \\ "normalization" \ "attributeMapping" \ "attribute").map { attribute =>
			val key = (attribute \ "key").text
			val values = (attribute \ "mapping").map(_.text).toList
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
			.map { case (normalized, notNormalized) =>
				val values = notNormalized.flatMap(entity.get)
				val normalizedValues = normalizeAttribute(normalized, values)
				(normalized, normalizedValues)
			}.filter(_._2.nonEmpty)
	}

	/**
	  * Filters the input entities and then transforms them to Subjects.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val path = args.headOption
		val configPath = path.orElse(this.configFile).getOrElse("datalakeimport_config.xml")
		this.parseConfig(configPath)
		val version = Version(appName, dataSources, sc)
		val mapping = parseNormalizationConfig(this.normalizationFile)
		input
			.fromAnyRDD[T]()
			.map(rdd =>
				rdd
					.filter(filterEntities)
					.map(translateToSubject(_, version, mapping)))
			.toAnyRDD()
	}
}
