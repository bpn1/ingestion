package de.hpi.ingestion.dataimport.kompass

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import play.api.libs.json._

object GraphExtraction {
	val inputFile = "kompass_parse_head.json"
	val separator = ";"
	val partitionNumber = 32

	def main(args : Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("GraphExtraction"))
		val jsonFile = sc.textFile(inputFile)

		jsonFile
			.map(line => {
				val json = Json.parse(line)
				val name = (json \ "name").as[String]
				val activities = (json \ "activities").as[List[JsValue]]
					.map(activity => (activity.as[String], name))

				activities
			})
			.flatMap(tupleList => tupleList)
			.groupByKey(numPartitions = partitionNumber)
			.map { case (activity, nameList) =>
				val edgeList = ListBuffer[String]()
				val unusedNameList = ListBuffer[String]() ++ nameList

				for(name1 <- nameList) {
					unusedNameList -= name1
					for(name2 <- unusedNameList) {
						edgeList += name1 + separator + activity + separator + name2
					}
				}

				edgeList.toList
			}
			.filter(_.size > 0)
			.flatMap(edgeList => edgeList)
			.saveAsTextFile("kompass_edges_" + System.currentTimeMillis / 1000)

		sc.stop
	}
}
