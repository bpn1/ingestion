package de.hpi.ingestion.dataimport.wikidata

import org.apache.spark.SparkContext
import java.util.UUID
import de.hpi.ingestion.implicits.CollectionImplicits._
import scala.util.matching.Regex
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.framework.SparkJob

/**
  * This job finds wikidata relations between subjects, translates the Wikidata Id relations into Subject UUID relations
  * and replaces the Wikidata Ids with their names.
  */
object FindRelations extends SparkJob {
	appName = "Find Relations v1.1"
	configFile = "wikidata_import.xml"
	val datasources = List("wikidata_20161117", "wikidata")

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](settings("subjectKeyspace"), settings("subjectTable"))
		List(subjects).toAnyRDD()
	}

	/**
	  * Saves the Subjects with the new relations to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(settings("subjectKeyspace"), settings("subjectTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Finds relations of a Subject to Wikidata ids, resolves the name of the ids and adds a
	  * relation to the subject with the corresponding Wikidata id.
	  * @param subject Subject to find the relations for
	  * @param nameResolveMap map used to resolve Wikidata ids into names and uuids
	  * @param version Version to use for versioning in the Subject Manager
	  * @return Subject with added relations and resolved Wikidata ids
	  */
	def findRelations(
		subject: Subject,
		nameResolveMap: Map[String, (UUID, String)],
		version: Version
	): Subject = {
		val sm = new SubjectManager(subject, version)
		val idRegex = new Regex("^Q[0-9]+$")
		var relationsMap = Map[UUID, Map[String, String]]()
		val resolvedProperties = subject.properties
		    .filterKeys(_ != settings("wikidataIdKey"))
			.map { case (property, propertyValues) =>
				val (resolvableValues, doneValues) = propertyValues.partition(value =>
					idRegex.findFirstIn(value).isDefined && nameResolveMap.contains(value))

				// add relations to subject with the corresponding wikidata id
				relationsMap ++= resolvableValues
					.map(value => (nameResolveMap(value)._1, Map(property -> "")))
					.toMap[UUID, Map[String, String]]
				val resolvedValues = resolvableValues
				    .map { id =>
						nameResolveMap.get(id)
						    .map(_._2)
					    	.filter(_.nonEmpty)
							.getOrElse(id)
					}
				(property, resolvedValues ++ doneValues)
			}
		sm.overwriteProperties(resolvedProperties)
		sm.addRelations(relationsMap)
		subject
	}

	/**
	  * Creates map containing the wikidata id as key and the corresponding subjects id and name
	  * as value.
	  * @param subjects RDD of subjects
	  * @return map of wikidata ids mapping to the corresponding subject id and name
	  */
	def resolvableNamesMap(subjects: RDD[Subject]): Map[String, (UUID, String)] = {
		subjects
			.filter(_.properties.get(settings("wikidataIdKey")).exists(_.nonEmpty))
			.map { subject =>
				val wikidataId = subject.properties(settings("wikidataIdKey")).head
				val name = subject.name.orElse(subject.aliases.headOption).getOrElse(wikidataId)
				(wikidataId, (subject.id, name))
			}.collect
			.toMap
	}

	/**
	  * Transforms the Wikidata relations to Subject relations.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val subjects = input.fromAnyRDD[Subject]().head
		val nameResolveMap = resolvableNamesMap(subjects)
		val version = Version(appName, datasources, sc, false, settings.get("subjectTable"))
		val subjectsWithRelations = subjects.map(findRelations(_, nameResolveMap, version))
		List(subjectsWithRelations).toAnyRDD()
	}
}
