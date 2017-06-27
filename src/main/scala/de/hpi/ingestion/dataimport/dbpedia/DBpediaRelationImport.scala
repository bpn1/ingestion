package de.hpi.ingestion.dataimport.dbpedia

import java.util.UUID
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Import and add the dbpedia relations to the dbpedia subjects.
  */
object DBpediaRelationImport extends SparkJob {
	appName = "DBpediaRelationImport"
	configFile = "relation_import_dbpedia.xml"


	// $COVERAGE-OFF$
	/**
	  * Loads DBpedia Subjects and DBpedia relations
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val dbpedia = sc.cassandraTable[Subject](settings("keyspaceDBpediaTable"), settings("dbpediaTable"))
		val relations = sc
			.cassandraTable[Relation](settings("keyspaceDBpediaRelationsTable"), settings("dbpediaRelationsTable"))
		List(dbpedia).toAnyRDD() ::: List(relations).toAnyRDD()
	}

	/**
	  * Saves the DBpedia Subjects to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(settings("keyspaceDBpediaTable"), settings("dbpediaTable"))
	}
	// $COVERAGE-ON$

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val version = Version("DBpediaRelationImport", List("dbpedia"), sc, false)
		val dbpedia = input.head.asInstanceOf[RDD[Subject]]
		val relations = input.last.asInstanceOf[RDD[Relation]]

		val dbpediaByName = dbpedia
			.collect { case subject: Subject if subject.name.isDefined => (subject.name.get, subject) }

		val relatedDBpedia = relations
			.keyBy(_.objectentity)
			.join(dbpediaByName)
			.values
			.map { case (Relation(subjectEntity, relationType, objectEntity), subject) =>
				(subjectEntity, List((relationType, subject.id)))
			}.reduceByKey(_ ::: _)
			.join(dbpediaByName)
			.values
			.map { case (subjectRelations, subject) =>
				addRelations(subject, subjectRelations, version)
				subject
			}

		List(relatedDBpedia).toAnyRDD()
	}

	/**
	  * Adds the relation defined in the relation object to the subject
	  * @param subjectEntity subject to which the relation should be added
	  * @param relations relation type and objectEntity Id
	  * @param version version of the Import
	  */
	def addRelations(subjectEntity: Subject, relations: List[(String, UUID)], version: Version): Unit = {
		val subjectManager = new SubjectManager(subjectEntity, version)

		val newRelations = relations
			.groupBy(_._2)
			.mapValues(_.map(_._1 -> "").toMap)

		subjectManager.addRelations(newRelations)
	}
}
