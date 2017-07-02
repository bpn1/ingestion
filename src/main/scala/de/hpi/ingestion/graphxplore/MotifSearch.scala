package de.hpi.ingestion.graphxplore

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, when, lit}
import org.graphframes.{GraphFrame, examples}

object MotifSearch {
	def sumFriends(count: Column, relationship: Column): Column = {
		when(relationship === "friend", count + 1).otherwise(count)
	}

	def main(args: Array[String]): Unit = {
		val g: GraphFrame = examples.Graphs.friends
		// find chains of 4 vertices
		val chainMotifs: DataFrame = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
		val edgeCount = Seq("ab", "bc", "cd")
			.foldLeft(lit(0))((count, e) => sumFriends(count, col(e)("relationship")))

		val twoFriendChains = chainMotifs.where(edgeCount >= 2)
		twoFriendChains.show()
	}
}
