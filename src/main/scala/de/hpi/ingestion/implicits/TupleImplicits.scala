package de.hpi.ingestion.implicits

import scala.language.{reflectiveCalls, implicitConversions}

/**
  * Contains the implicit classes for tuple type methods.
  */
object TupleImplicits {

	/**
	  * Adds the map function to the Tuple2 class.
	  * @param t the tuple on which the method will be called.
	  * @tparam A type of the first tuple element
	  * @tparam B type of the second tuple element
	  */
	implicit class Mappable[A, B](t: (A, B)) {
		/**
		  * Applies two different functions to the two tuple elements and returns the result.
		  * Source: http://stackoverflow.com/a/4022510
		  * @param f function applied to the first tuple element
		  * @param g function applied to the second tuple element
		  * @tparam R return type of the first function f
		  * @tparam S return type of the second function g
		  * @return the result of f and g as tuple
		  */
		def map[R, S](f: A => R, g: B => S): (R, S) = (f(t._1), g(t._2))
	}

}
