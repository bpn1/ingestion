package de.hpi.ingestion.datalake

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import java.net.URL
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.datalake.models.{DLImportEntity, Subject, Version}

import scala.io.Source
import scala.xml.XML

trait DLImport[T <: DLImportEntity] extends Serializable {
	val outputKeyspace = "datalake"
	val outputTable = "subject_temp"
	val versionTable = "version"

	/**
	  * Reads the data of the datasource to import and transforms the entries to Subjects.
	  *
	  * @param sc      the SparkContext of the program
	  * @param version the version of the new Subjects
	  * @return a RDD of new Subjects created from the given objects
	  */
	protected def readInput(sc: SparkContext, version: Version): RDD[Subject]

	/**
	  * Translates an entity of the datasource into a Subject, which is the schema of the staging table.
	  * @param entity  the object to be converted to a Subject
	  * @param version the version of the new Subject
	  * @return a new Subject created from the given object
	  */
	protected def translateToSubject(entity: T, version: Version): Subject

	/**
	  * Parses the normalization config file into a Map.
	  * @param url the path to the config file
	  * @return a Map containing the normalized attributes mapped to the new subject attributes
	  */
	protected def parseNormalizationConfig(url: URL): Map[String, List[String]]

	/**
	  * Parses the normalization config file into a Map
	  * @param path the path to the config file
	  * @return a Map containing the normalized attributes mapped to the new subject attributes
	  */
	protected def parseNormalizationConfig(path: String): Map[String, List[String]]

	/**
	  * Normalizes a given entity into a map
	  * @param entity the object to be normalized
	  * @return a Map containing the normalized information of the entity
	  */
	protected def normalizeProperties(
		entity: T,
		mapping: Map[String, List[String]]
	): Map[String, List[String]]

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
  * @param appName       		the name of the importing job
  * @param dataSources   		list of the sources where the new data is fetched from
  * @param normalizationFile 	name of normalization file in resource folder
  * @param inputKeyspace 		the name of the keyspace where the new data is saved in the database
  * @param inputTable    		the name of the table where the new data is saved in the database
  * @tparam T the type of Objects of the new data
  */
abstract case class DataLakeImport[T <: DLImportEntity](
	appName: String,
	dataSources: List[String],
	normalizationFile: String,
	inputKeyspace: String,
	inputTable: String
) extends DLImport[T] {
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
			}
			.toMap
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
