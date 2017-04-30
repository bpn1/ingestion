package de.hpi.ingestion.implicits

import org.apache.spark.rdd.RDD

/**
  * Contains implicit classes extending Scala collections.
  */
object CollectionImplicits {

	/**
	  * Implicit class which adds the cross product to classes implementing the Traversable trait.
	  * @param xs the Collection calling the method
	  * @tparam X the type of the objects contained in the collection
	  */
	implicit class Crossable[X](xs: Traversable[X]) {
		/**
		  * Returns the cross product of both collections: xs X ys.
		  * Source: http://stackoverflow.com/a/14740340
		  * @param ys the collection to create the cross product with
		  * @tparam Y the type of the objects contained in the collection
		  * @return the cross product of xs and ys
		  */
		def cross[Y](ys: Traversable[Y]): Traversable[(X, Y)] = for { x <- xs; y <- ys } yield (x, y)
	}

	/**
	  * Implicit class which adds printing of the set difference to another collection to classes implementing the
	  * Traversable trait.
	  * @param xs the Collection calling the method
	  * @tparam X the type of the objects contained in the collection
	  */
	implicit class SetDifference[X](xs: Traversable[X]) {
		/**
		  * Prints the set difference of the collection calling this method xs and the parameter ys.
		  * @param ys the collection used to create the difference
		  * @tparam Y the type of the objects contained in the collection
		  */
		def printableSetDifference[Y](ys: Traversable[Y]): String = {
			val diff1 = s"x - y:\n\t${xs.toSet.filterNot(ys.toSet).mkString("\n\t")}"
			val diff2 = s"y - x:\n\t${ys.toSet.filterNot(xs.toSet).mkString("\n\t")}"
			s"Difference:\n$diff1\n$diff2"
		}
	}

	/**
	  * Implicit class which adds the casting of a collection of RDDs of a single type to a collection of RDDs of Any.
	  * @param xs Collection containing the RDDs
	  * @tparam X type of the RDDs
	  */
	implicit class ToAnyRDD[X](xs: List[RDD[X]]) {
		/**
		  * Casts the RDDs to RDDs of type Any.
		  * @return Collection of RDDs of type Any
		  */
		def toAnyRDD(): List[RDD[Any]] = xs.map(_.asInstanceOf[RDD[Any]])
	}

	/**
	  * Implicit class which adds the casting of a collection of RDDs of Any to a collection of RDDs of a single type .
	  * @param xs Collection containing the RDDs of type Any
	  */
	implicit class FromAnyRDD(xs: List[RDD[Any]]) {
		/**
		  * Casts the RDDs of type Any to the given type.
		  * @tparam X type of the RDDs they will be cast to
		  * @return Collection of RDDs of the given type
		  */
		def fromAnyRDD[X](): List[RDD[X]] = xs.map(_.asInstanceOf[RDD[X]])
	}


}
