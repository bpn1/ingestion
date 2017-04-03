package de.hpi.ingestion.textmining

import org.apache.spark.rdd.RDD

trait PrettyTester {
	def printSequenceAsError[T](sequence: Seq[T], title: String = ""): Unit = {
		Console.err.println(s"$title\n${sequence.mkString("\n")}")
	}

	def areRDDsEqual[T](is: RDD[T], should: RDD[T]): Boolean = {
		var areEqual = is.count == should.count
		if (areEqual) {
			areEqual = is
				.collect
				.zip(should.collect)
				.collect { case (a, b) if a != b => a -> b } // difference RDD between both RDDs
				.isEmpty
		}

		if (!areEqual) {
			printSequenceAsError(is.collect, "\nRDD:")
			printSequenceAsError(should.collect, "\nShould be:")
		}
		areEqual
	}

	def isSubset[T](subset: Set[T], set: Set[T]): Boolean = {
		val isSubset = subset.subsetOf(set)

		if (!isSubset) {
			printSequenceAsError(subset.toSeq, "\nSet:")
			printSequenceAsError(set.toSeq, "\nShould be subset of:")
		}
		isSubset
	}

	def areSetsEqual[T](is: Set[T], should: Set[T]): Boolean = {
		val areEqual = is == should

		if (!areEqual) {
			printSequenceAsError(is.toSeq, "\nSet:")
			printSequenceAsError(should.toSeq, "\nShould be:")
		}
		areEqual
	}

	def areListsEqual[T](is: List[T], should: List[T]): Boolean = {
		val areEqual = is == should

		if (!areEqual) {
			printSequenceAsError(is, "\nList:")
			printSequenceAsError(should, "\nShould be:")
		}
		areEqual
	}
}
