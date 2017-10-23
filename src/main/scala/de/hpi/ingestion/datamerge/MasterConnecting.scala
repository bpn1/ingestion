/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.datamerge

import java.util.UUID

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.SubjectManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.framework.SparkJob

/**
  * Updating relations of the master node to new master nodes
  */
class MasterConnecting extends SparkJob {
	import MasterConnecting._
	appName = "MasterConnecting"
	configFile = "merging_master_connecting.xml"

	var subjects: RDD[Subject] = _
	var connectedMasters: RDD[Subject] = _

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects, the staged Subjects and the Duplicate Candidates from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
	}

	/**
	  * Saves the merged Subjects to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		connectedMasters.saveToCassandra(settings("keyspaceSubjectTable"), settings("subjectTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Connect master nodes.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val masters = subjects.filter(master => master.isMaster && master.masterRelations.nonEmpty)
		val slaves = subjects.filter(_.isSlave).map(slave => (slave.id, (slave.id, slave.master, slave.datasource)))
		val version = Version(appName, List("subject"), sc, true, settings.get("subjectTable"))

		connectedMasters = masters
			.flatMap(extractRelations)
			.reduceByKey(_ ::: _)
			.join(slaves)
			.values
			.flatMap((groupByMaster _).tupled)
			.reduceByKey(_ ::: _)
			.map { case (master, relationsAndIds) =>
				val masterManager = new SubjectManager(master, version)
				val oldIds = relationsAndIds.map(_._2)
				val updatedRelations = relationsAndIds
					.flatMap { case (relationMap, oldId, datasource) =>
						relationMap.toList.map { case (id, relationProperties) => (id, relationProperties, datasource) }
					}.groupBy(_._1)
					.mapValues { valueList =>
						Merging.sourcePriority.map { datasource =>
							valueList
								.filter(_._3 == datasource)
								.map { case (id, relationProperties, datasource) => relationProperties }
								.foldLeft(Map.empty[String, String])(_ ++ _)
						}.foldLeft(Map.empty[String, String])(
							(higherPriorityRels, lowerPriorityRels) => lowerPriorityRels ++ higherPriorityRels)
					}.map(identity)
				masterManager.removeRelations(oldIds)
				masterManager.addRelations(updatedRelations)
				master
			}
	}
}

object MasterConnecting {
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
	  * Groups the relations by the masters in which they are stored
	  * @param relations relations pointing to the slaves as List of tuples
	  * @param slaveTuple Tuple of (slaveId, masterId, datasource)
	  * @return List of Tuples (master, updated relations with slave id and slave datasource)
	  */
	def groupByMaster(
		relations: List[(Subject, Map[String, String])],
		slaveTuple: (UUID, UUID, String)
	): List[(Subject, List[(Map[UUID, Map[String, String]], UUID, String)])] = {
		val (oldId, relationMasterId, datasource) = slaveTuple
		relations.map { case (master, relationProps) =>
			(master, List((Map(relationMasterId -> relationProps), oldId, datasource)))
		}
	}
}
