/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

		/**
		  * Returns the cross product of this collection with itself without symmetrical (duplicate) tuples.
		  * By default the reflexive pairs are also filtered.
		  * @param reflexive whether or not the reflexive pairs are kept
		  * @return filtered cross product of xs with itself
		  */
		def asymSquare(reflexive: Boolean = false): Traversable[(X, X)] = {
			val data = xs.toList
			val offset = if(reflexive) 0 else 1
			data
				.indices
				.flatMap(i => xs.slice(i + offset, xs.size).map((data(i), _)))
		}
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

	/**
	  * Implicit class which adds the counting of a collections elements.
	  * @param xs Collection containing the elements to count
	  * @tparam X type of the Collection
	  */
	implicit class CountElements[X](xs: Traversable[X]) {
		/**
		  * Counts the elements of the Collection calling this method.
		  * @return Map containing the counts for each element of this collection
		  */
		def countElements(): Map[X, Int] = {
			xs
				.groupBy(identity)
				.mapValues(_.size)
				.map(identity)
		}
	}

	/**
	  * Adds functions to Maps.
	  * @param xs the Map itself
	  * @tparam X the type of the keys
	  * @tparam Y the type of the values
	  */
	implicit class MapFunctions[X, Y](xs: Map[X, Y]) {
		/**
		  * Applies a function to all keys of this map. The resulting Map may be smaller if there are collisions.
		  * @param f function applied to every key
		  * @tparam Z return type of the function f
		  * @return Map with the function f applied to each key
		  */
		def mapKeys[Z](f: (X) => Z): Map[Z, Y] = xs.map { case (key, value) => (f(key), value) }
	}
}
