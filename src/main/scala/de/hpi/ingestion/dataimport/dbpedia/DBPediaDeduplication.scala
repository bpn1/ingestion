package de.hpi.ingestion.dataimport.dbpedia

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.dataimport.dbpedia.models.DBPediaEntity

object DBPediaDeduplication {

	val appname = "DBPediaImport_v1.0"
	val dataSources = List("dbpedia_20161203")

	def main(args: Array[String]) {
		val conf = new SparkConf()
			.setAppName(appname)

		val sc = new SparkContext(conf)

		val dbpedia = sc.cassandraTable[DBPediaEntity]("wikidumps", "dbpediadata")
		val subjects = sc.cassandraTable[Subject]("datalake", "subject")

		val pairDBpedia = dbpedia.keyBy(_.wikipageid)
		val pairSubject = subjects
		  .filter(subject => subject.properties.get("wikipageId").isDefined)
		  .keyBy(_.properties.getOrElse("wikipageId", List("null")).head)
		//val joinedTable = pairDBpedia.leftOuterJoin(pairSubject)
		//joinedTable.takeSample(false, 10).foreach(println)

		sc.stop()
	}
}
