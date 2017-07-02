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
		println("=> Chains of four nodes with at least 2 friends:")
		twoFriendChains.show()

		// find all paths between Esther and David
		val nodePaths: DataFrame = g.bfs
			.fromExpr("name = 'Esther'")
	    	.toExpr("name = 'Alice' or age < 32")
			.run()

		val filteredNodePaths: DataFrame = g.bfs
			.fromExpr("name = 'Esther'")
			.toExpr("age < 32")
			.edgeFilter("relationship != 'friend'")
			.maxPathLength(5)
			.run()

		println("=> Paths from Esther to Alice and people younger than 32:")
		nodePaths.show()
		println("=> Paths from Esther to people under 32 without friend relations")
		filteredNodePaths.show()
	}
}
