package de.hpi.ingestion.deduplication

import java.util.UUID
import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object GoldStandard {
	val appName = "GoldStandard_v1.0"
	val dataSource = List("DBPedia", "Wikidata")
	val keyspace = "evaluation"
	val inputDBPedia = "subject_dbpedia"
	val inputWikiData = "subject_wikidata"
	val output = "goldstandard"

	/**
	  * Keys Subjects by a property value
	  * @param rdd Subjects
	  * @param property Property to key by
	  * @return RDD containing Subjects keyed by a value
	  */
	def keyBySingleProperty(rdd: RDD[Subject], property: String): RDD[(String, Subject)] = {
		rdd
			.filter(_.properties.contains(property))
			.map { subject =>
				val keyOpt = subject.properties(property).headOption
				(keyOpt.getOrElse(""), subject)
			}
	}

	/**
	  * Join DBPedia and WikiData together on a property.
	  * @param property The property to be joined on.
	  * @param rdds The rdds to be joined
	  * @return RDD containing pairs of UUIDs
	  */
	def joinBySingleProperty(property: String)(implicit rdds: (RDD[Subject], RDD[Subject])): RDD[(UUID, UUID)] = {
		val (dbpedia, wikidata) = rdds
		val propertyKeyDBPedia = keyBySingleProperty(dbpedia, property)
		val propertyKeyWikiData = keyBySingleProperty(wikidata, property)
		propertyKeyDBPedia
		    .join(propertyKeyWikiData)
		    .mapValues { case (dbpediaSubject, wikidataSubject) =>
				(dbpediaSubject.id, wikidataSubject.id)
			}
		    .values
	}

	/**
	  * Join DBPedia and WikiData together based on ids
	  * @param dbpedia Subjects from DBPedia
	  * @param wikidata Subjects from WikiData
	  * @return RDD containing pairs of UUIDs
	  */
	def join(dbpedia: RDD[Subject], wikidata: RDD[Subject]): RDD[(UUID, UUID)] = {
		implicit val rdds = (dbpedia, wikidata)
		val dbpediaIdJoined = joinBySingleProperty("id_dbpedia")
		val wikidataIdJoined = joinBySingleProperty("id_wikidata")
		dbpediaIdJoined.union(wikidataIdJoined).distinct
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName(this.appName)
		val sc = new SparkContext(conf)

		val version = Version(this.appName, this.dataSource, sc)
		val dbpedia = sc.cassandraTable[Subject](this.keyspace, this.inputDBPedia)
		val wikidata = sc.cassandraTable[Subject](this.keyspace, this.inputWikiData)

		join(dbpedia, wikidata).saveToCassandra(this.keyspace, this.output)
	}
}
