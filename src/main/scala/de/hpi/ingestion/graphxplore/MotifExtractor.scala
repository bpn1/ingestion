package de.hpi.ingestion.graphxplore

import java.util.UUID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.graphframes.GraphFrame
import de.hpi.ingestion.graphxplore.models.ResultGraph

object MotifExtractor extends GraphExtractor {
	appName = "MotifExtractor"
	val minStarSize = 4 // 3
	val maxStarSize = 4 // can't be > 26

	def findMotifs(graph: GraphFrame, query: String, nodeNames: List[String], graphType: String): RDD[ResultGraph] = {
		val motifs = graph.find(query)

		val idColumns = nodeNames.map(name => col(name.toString + ".id"))
		val nameColumns = nodeNames.map(name => col(name.toString + ".name"))

		motifs
			.select(idColumns ++ nameColumns: _*)
			.rdd
			.map(_.toSeq.toList)
			.filter(_.forall(_ != null))
			.map(_.map(_.toString))
			.map(columns => {
				val ids = columns.take(nodeNames.size).map(UUID.fromString)
				val names = columns.slice(nodeNames.size, columns.size)
				ids.zip(names)
			})
			.map(idNameTuples => ResultGraph(
				graphType,
				idNameTuples.map(_._1),
				idNameTuples.filter(_._2 != null).map(_._2)))
	}

	/**
	  * Finds pre-defined motifs in the given graph
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted company groups
	  */
	override def processGraph(graph: GraphFrame): List[RDD[ResultGraph]] = {
		var resultGraphs = List[RDD[ResultGraph]]()

		for(starSize <- minStarSize to maxStarSize) {
			val nodeNames = ('a' to 'z').take(starSize).map(_.toString).toList :+ "center"
			val motifQuery = nodeNames.map(char => s"($char)-[]->(center)").mkString("; ")
			resultGraphs :+= findMotifs(graph, motifQuery, nodeNames, "Star")
		}

		val chainQuery = "(a)-[]->(b); (b)-[]->(c); (c)-[]->(d)"
		val chainNames = List("a", "b", "c", "d")
		resultGraphs :+= findMotifs(graph, chainQuery, chainNames, "Chain")

		resultGraphs
	}
}
