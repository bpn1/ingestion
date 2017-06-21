package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation

/**
  * Exports the Cooccurrences to Neo4j CSV.
  */
object CooccurrenceExport extends SparkJob {
	appName = "Co-Occurrence Export"
	configFile = "textmining.xml"
	val separator = ","
	val quote = "\""
	sparkOptions("spark.yarn.executor.memoryOverhead") = "5G"

	// $COVERAGE-OFF$
	/**
	  * Loads Cooccurrences from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val cooccurrences = sc.cassandraTable[Cooccurrence](settings("keyspace"), settings("cooccurrenceTable"))
		val relations = sc.cassandraTable[Relation](settings("keyspace"), settings("DBpediaRelationTable"))
		List(cooccurrences).toAnyRDD() ++ List(relations).toAnyRDD()
	}

	/**
	  * Saves the CSV files to the HDFS.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val List(nodes, edges) = output.fromAnyRDD[String]()
		nodes.saveAsTextFile(s"relation_nodes_${System.currentTimeMillis()}")
		edges.saveAsTextFile(s"relation_edges_${System.currentTimeMillis()}")
	}
	// $COVERAGE-ON$

	/**
	  * Exports the Co-Occurrence nodes and edges to CSV.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val cooccurrences = input.fromAnyRDD[Cooccurrence]().head
		val relations = input.fromAnyRDD[Relation]().last
		val cleanedCooccurrences = cooccurrences.map { occurrence =>
			val cleanedEntities = occurrence.entitylist.map(_.replaceAll("\"", "\\\\\""))
			occurrence.copy(entitylist = cleanedEntities)
		}
		val cleanedRelations = relations.map { relation =>
			val cleanedSubject = relation.subjectentity.replaceAll("\"", "\\\\\"").trim
			val cleanedObject = relation.objectentity.replaceAll("\"", "\\\\\"").trim
			relation.copy(subjectentity = cleanedSubject, objectentity = cleanedObject)
		}
		val nodesCooc = cleanedCooccurrences
			.flatMap(_.entitylist.map(_.trim))

		val edgesCooc = cleanedCooccurrences
			.flatMap { case Cooccurrence(entities, count) =>
				entities.asymSquare().map((_, count))
			}.reduceByKey(_ + _)
			.map { case ((start, end), count) =>
				s"${quote}${start}${quote},${count},${quote}${end}${quote},CO_OCCURRENCE"
			}.distinct
		val nodesRel = cleanedRelations.flatMap(rel => List(rel.subjectentity.trim, rel.objectentity.trim))
		val nodes = (nodesCooc ++ nodesRel)
			.map(node => s"${quote}${node}${quote},${quote}${node}${quote},Entity")
			.distinct
		val edgesRel = cleanedRelations
			.map { case Relation(subjectentity, relationtype, objectentity) =>
				s"${quote}${subjectentity}${quote},${relationtype},${quote}${objectentity}${quote},DBPEDIA"
			}.distinct
		val edges = edgesCooc ++ edgesRel
		List(nodes, edges).toAnyRDD()
	}
}
