import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class PrettyTester extends FlatSpec {
	def printSequence[T](sequence: Seq[T], title: String = ""): Unit = {
		println(s"$title\n${sequence.mkString("\n")}")
	}

	def areRDDsEqual[T](is: RDD[T], should: RDD[T]): Boolean = {
		var areEqual = is.count == should.count
		if (areEqual) {
			areEqual = is
				.collect
				.zip(should.collect)
				.collect { case (a, b) if a != b => a -> b }
				.isEmpty
		}

		if (!areEqual) {
			printSequence(is.collect, "\nRDD:")
			printSequence(should.collect, "\nShould be:")
		}
		areEqual
	}

	def isSubset[T](subset: Set[T], set: Set[T]): Boolean = {
		val isSubset = subset.subsetOf(set)

		if (!isSubset) {
			printSequence(subset.toSeq, "\nSet:")
			printSequence(set.toSeq, "\nShould be subset of:")
		}
		isSubset
	}

	def areSetsEqual[T](is: Set[T], should: Set[T]): Boolean = {
		val areEqual = is == should

		if (!areEqual) {
			printSequence(is.toSeq, "\nSet:")
			printSequence(should.toSeq, "\nShould be:")
		}
		areEqual
	}

	def areListsEqual[T](is: List[T], should: List[T]): Boolean = {
		val areEqual = is == should

		if (!areEqual) {
			printSequence(is, "\nSet:")
			printSequence(should, "\nShould be:")
		}
		areEqual
	}
}
