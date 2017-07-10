package de.hpi.ingestion.curation

import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.DuplicateCandidates
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Commit extends SparkJob {
  appName = "Commit"

  /**
    * Loads a number of input RDDs used in the job.
    *
    * @param sc   Spark Context used to load the RDDs
    * @param args arguments of the program
    * @return List of RDDs containing the data processed in the job.
    */
  override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
    val subjects = sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable"))
    List(subjects).toAnyRDD()
  }

  /**
    * Executes the data processing of this job and produces the output data.
    *
    * @param input List of RDDs containing the input data
    * @param sc    Spark Context used to e.g. broadcast variables
    * @param args  arguments of the program
    * @return List of RDDs containing the output data
    */
  override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
    val subjects = input.take(1).fromAnyRDD[Subject]()
  }

  /**
    * Saves the output data to e.g. the Cassandra or the HDFS.
    *
    * @param output List of RDDs containing the output of the job
    * @param sc     Spark Context used to connect to the Cassandra or the HDFS
    * @param args   arguments of the program
    */
  override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {

  }
}
