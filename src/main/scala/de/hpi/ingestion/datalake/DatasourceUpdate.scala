package de.hpi.ingestion.datalake

import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.datamerge.Merging
import de.hpi.ingestion.implicits.CollectionImplicits._

object DatasourceUpdate extends SparkJob {
	appName = "Datasource Update"
	configFile = "datasource_update.xml"

	// $COVERAGE-OFF$
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val newSubjects = sc.cassandraTable[Subject](settings("inputKeyspace"), settings("inputTable"))
		val subjects = sc.cassandraTable[Subject](settings("outputKeyspace"), settings("outputTable"))
		List(newSubjects, subjects).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
	    	.fromAnyRDD[Subject]()
	    	.head
	    	.saveToCassandra(settings("outputKeyspace"), settings("outputTable"))
	}
	// $COVERAGE-ON$

	def updateSubject(oldSubject: Subject, newSubject: Subject, version: Version): Subject = {
		val sm = new SubjectManager(oldSubject, version)
		sm.setName(newSubject.name)
		sm.addAliases(newSubject.aliases)
		sm.setCategory(newSubject.category)
		sm.overwriteProperties(newSubject.properties)
		sm.overwriteRelations(newSubject.relations)
		oldSubject
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val List(newSubjects, subjects) = input.fromAnyRDD[Subject]()
		val version = Version(appName, List("datasourceupdate"), sc, false, settings.get("inputTable"))
		val updateAbleSubjects = subjects
			.filter(_.isSlave)
			.flatMap { subject =>
				val keys = subject.get(settings("datasourceKey"))
				keys.map(key => (key, subject))
			}
		val updatedSubjects = newSubjects
	    	.flatMap { subject =>
				val keys = subject.get(settings("datasourceKey"))
				keys.map(key => (key, subject))
			}.leftOuterJoin(updateAbleSubjects)
			.values
			.flatMap {
				case (newSubject, Some(oldSubject)) => List(updateSubject(oldSubject, newSubject, version))
				case (newSubject, None) => Merging.addToMasterNode(newSubject, version).map(_._1)
			}
		List(updatedSubjects).toAnyRDD()
	}
}
