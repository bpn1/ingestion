import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class PrettyTester extends FlatSpec {
	def printSequence(sequence: Seq[Any], title: String = ""): Unit = {
		println(title)
		sequence
			.foreach(println)
	}

	def areRDDsEqual(is: RDD[Any], should: RDD[Any]): Boolean = {
		var areEqual = true
		val sizeIs = is.count
		val sizeShould = should.count
		if (sizeIs != sizeShould)
			areEqual = false
		if (areEqual) {
			val diff = is
				.collect
				.zip(should.collect)
				.collect { case (a, b) if a != b => a -> b }
			areEqual = diff.isEmpty
		}

		if (!areEqual) {
			printSequence(is.collect, "\nRDD:")
			printSequence(should.collect, "\nShould be:")
		}
		areEqual
	}

	def isSubset(subset: Set[Any], set: Set[Any]): Boolean = {
		val isSubset = subset.subsetOf(set)

		if (!isSubset) {
			printSequence(subset.toSeq, "\nSet:")
			printSequence(set.toSeq, "\nShould be subset of:")
		}
		isSubset
	}

	def areSetsEqual(is: Set[Any], should: Set[Any]): Boolean = {
		val areEqual = is == should

		if (!areEqual) {
			printSequence(is.toSeq, "\nSet:")
			printSequence(should.toSeq, "\nShould be:")
		}
		areEqual
	}

	def areListsEqual(is: List[Any], should: List[Any]): Boolean = {
		val areEqual = is == should

		if (!areEqual) {
			printSequence(is, "\nSet:")
			printSequence(should, "\nShould be:")
		}
		areEqual
	}
}
