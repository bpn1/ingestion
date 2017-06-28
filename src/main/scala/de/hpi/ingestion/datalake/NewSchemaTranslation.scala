package de.hpi.ingestion.datalake

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.datalake.models.{Subject, Old_Subject}

/**
  * Created by Lando on 27.06.17.
  */
object NewSchemaTranslation extends SparkJob {
	appName = "NewSchemaTranslation"
	configFile = "new_schema_translation.xml"

	// $COVERAGE-OFF$
	/**
	  * Load subject
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = sc.cassandraTable[Old_Subject](settings("inputKeyspace"), settings("inputTable"))
		List(subjects).toAnyRDD()
	}

	/**
	  * Saves the Subjects entities to the Cassandra.
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

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val oldSubjects = input.head.asInstanceOf[RDD[Old_Subject]]
		val datasource = settings("datasource")

		val subjects = oldSubjects.map { oldSubject =>
			Subject
				.empty(oldSubject.id, datasource)
				.copy(
					name = oldSubject.name,
					aliases = oldSubject.aliases,
					category = oldSubject.category,
					properties = oldSubject.properties,
					relations = oldSubject.relations,

					name_history = oldSubject.name_history,
					aliases_history = oldSubject.aliases_history,
					category_history = oldSubject.category_history,
					properties_history = oldSubject.properties_history,
					relations_history = oldSubject.relations_history
				)
		}
		List(subjects).toAnyRDD()
	}
}
