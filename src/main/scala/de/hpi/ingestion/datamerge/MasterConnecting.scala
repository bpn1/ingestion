package de.hpi.ingestion.datamerge

import java.util.UUID

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._

/**
  * Updating relations of the master node to new master nodes
  */
object MasterConnecting extends SparkJob {
	appName = "MasterConnecting"
	configFile = "merging_master_connecting.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects, the staged Subjects and the Duplicate Candidates from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
		List(subjects).toAnyRDD()
	}

	/**
	  * Saves the merged Subjects to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Subject]()
			.head
			.saveToCassandra(settings("keyspaceSubjectTable"), settings("subjectTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Connect master nodes.
	  * @param input List of RDDs containing the subjects
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the updated master subjects
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = input.head.asInstanceOf[RDD[Subject]]
		val masters = subjects.filter(master => master.isMaster && master.masterRelations.nonEmpty)
		val slaves = subjects.filter(_.isSlave).map(slave => (slave.id, slave.master))
		val version = Version("Master Connecting", List("subject"), sc, true, settings.get("subjectTable"))


		val updatedMasters = masters
			.flatMap(extractRelations)
			.reduceByKey(_ ::: _)
			.join(slaves)
			.flatMap(Function.tupled(groupByMaster _))
			.reduceByKey(_ ::: _)
			.map { case (master, list) =>
				val masterManager = new SubjectManager(master, version)
				val (relations, ids) = list.unzip

				masterManager.removeRelations(ids)
				masterManager.addRelations(relations.reduceLeft(_ ++ _))
				master
			}

		List(updatedMasters).toAnyRDD()
	}

	/**
	  * Extract relations and group by id.
	  * @param master subject to extract relations from
	  * @return Map containing relations
	  */
	def extractRelations(master: Subject): Map[UUID, List[(Subject, Map[String, String])]] = {
		master
			.masterRelations
			.map { case (id, relations) =>
				(id, List((master, relations.map(identity))))
			}
	}

	/**
	  * Group relations by masters
	  * @param oldId id of slave the relation points to
	  * @param relationsTuple list of relations to be updated and the id of the master node
	  * @return list of masters with their updated relations
	  */
	def groupByMaster(
		oldId: UUID,
		relationsTuple: (List[(Subject, Map[String, String])], UUID)
	): List[(Subject, List[(Map[UUID, Map[String, String]], UUID)])] = {
		val (list, newId) = relationsTuple
		list.map { case (master, relations) =>
			(master, List((Map(newId -> relations), oldId)))
		}
	}
}
