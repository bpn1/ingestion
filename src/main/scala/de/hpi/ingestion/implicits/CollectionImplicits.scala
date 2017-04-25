package de.hpi.ingestion.implicits

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
	  * Implicit class which adds printing of the difference to another collection to classes implementing the
	  * Traversable trait.
	  * @param xs the Collection calling the method
	  * @tparam X the type of the objects contained in the collection
	  */
	implicit class SetDifference[X](xs: Traversable[X]) {
		/**
		  * Prints the difference of the collection calling this method xs and the parameter ys.
		  * @param ys the collection used to create the difference
		  * @tparam Y the type of the objects contained in the collection
		  */
		def printDifference[Y](ys: Traversable[Y]): Unit = {
			println(s"x - y:\n\t${xs.toSet.filterNot(ys.toSet).mkString("\n\t")}")
			println(s"y - x:\n\t${ys.toSet.filterNot(xs.toSet).mkString("\n\t")}")
		}
	}
}
