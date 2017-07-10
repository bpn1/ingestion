package de.hpi.ingestion.graphxplore

import java.util.UUID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.graphframes.GraphFrame
import de.hpi.ingestion.graphxplore.models.ResultGraph

object MotifExtractor extends GraphExtractor {
	appName = "MotifExtractor"
	val graphType = "Motif"
	val minStarSize = 3
	val maxStarSize = 5 // can't be > 26

	/**
	  * Finds pre-defined motifs in the given graph
	  * @param graph GraphFrame that is processed
	  * @return RDD of ResultGraph objects that represent extracted company groups
	  */
	override def processGraph(graph: GraphFrame): List[RDD[ResultGraph]] = {
		var resultMotifs = List[RDD[ResultGraph]]()

		for(starSize <- minStarSize to maxStarSize) {
			val nodeNames = ('a' to 'z').take(starSize)
			val motifQuery = nodeNames.map(char => s"($char)-[]->(center)").mkString("; ")
			val motifs = graph.find(motifQuery)

			val idColumns = nodeNames.map(name => col(name.toString + ".id"))
			val nameColumns = nodeNames.map(name => col(name.toString + ".name"))

			motifs
				.select(idColumns ++ nameColumns: _*)
				.rdd
				.map(_.toSeq.toList.map(_.toString))
				.map(columns => {
					val ids = columns.take(starSize).map(UUID.fromString)
					val names = columns.slice(starSize, columns.size)
					ids.zip(names)
				})
				.map(idNameTuples => ResultGraph(
					"Star",
					idNameTuples.map(_._1),
					idNameTuples.filter(_._2 != null).map(_._2)))

			resultMotifs :+= motifs
		}

		resultMotifs
	}
}
