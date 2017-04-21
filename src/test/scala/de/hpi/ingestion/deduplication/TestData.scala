package de.hpi.ingestion.deduplication

import java.util.UUID
import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TestData {
	val dbpediaList = List(
			Subject(
				name = Option("dbpedia_1"),
				properties = Map("id_dbpedia" -> List("dbpedia_1"), "id_wikidata" -> List("Q1"))
			),
			Subject(
				name = Option("dbpedia_2"),
				properties = Map("id_dbpedia" -> List("dbpedia_2"), "id_wikidata" -> List("Q2"))
			),
			Subject(
				name = Option("dbpedia_3"),
				properties = Map("id_wikidata" -> List("Q3"))
			),
			Subject(name = Option("dbpedia_4"))
	)

	val wikidataList = List(
			Subject(
				name = Option("wikidata_1"),
				properties = Map("id_dbpedia" -> List("dbpedia_1"), "id_wikidata" -> List(""))
			),
			Subject(
				name = Option("wikidata_2"),
				properties = Map("id_dbpedia" -> List("dbpedia_2"), "id_wikidata" -> List("Q2"))
			),
			Subject(
				name = Option("wikidata_3"),
				properties = Map("id_dbpedia" -> List("dbpedia_3"), "id_wikidata" -> List("Q3"))
			),
			Subject(name = Option("wikidata_4"))
	)

	def propertyKeyDBPedia(sc: SparkContext): RDD[(String, Subject)] = {
		sc.parallelize(Seq(
			("dbpedia_1", dbpediaList.head),
			("dbpedia_2", dbpediaList(1))
		))
	}

	def joinedWikiData(sc: SparkContext): RDD[(UUID, UUID)] = {
		sc.parallelize(Seq(
			(dbpediaList(1).id, wikidataList(1).id),
			(dbpediaList(2).id, wikidataList(2).id)
		))
	}

	def joinedDBPediaWikiData(sc: SparkContext): RDD[(UUID, UUID)] = {
		sc.parallelize(Seq(
			(dbpediaList.head.id, wikidataList.head.id),
			(dbpediaList(1).id, wikidataList(1).id),
			(dbpediaList(2).id, wikidataList(2).id)
		))
	}
}
