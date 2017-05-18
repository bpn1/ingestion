package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.jsoup.Jsoup
import play.api.libs.json.Json
import com.datastax.spark.connector._

object SpiegelImport extends SparkJob {

	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val spiegel = sc.textFile("spiegel.json")
		List(spiegel).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[SpiegelArticle]()
			.head
			.saveToCassandra("", "")
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		input
			.fromAnyRDD[String]()
			.map(spiegel =>
				spiegel.map { rawArticle =>
					val json = Json.parse(rawArticle)
					val id = (json \ "_id" \ "$oid").as[String]
					val htmlDoc = Jsoup.parse((json \ "content").as[String])
					SpiegelArticle(id, Option(htmlDoc.body().text())
			}).toAnyRDD()
	}
}
