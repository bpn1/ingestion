package de.hpi.ingestion.textmining.models

import scala.math.BigDecimal
import scala.language.implicitConversions

/**
  * Describes constraints for data type of the value type for Bags.
  * @tparam T type parameter
  */
trait BagCounter[T] extends Ordered[T] {
	/**
	  * Adds this BagCounter to another BagCounter of the same type.
	  * @param that other BagCounter of the same type
	  * @return sum of the two BagCounters
	  */
	def +(that: T): T

	/**
	  * Subtracts another BagCounter of the same type from this BagCounter.
	  * @param that other BagCounter of the same type
	  * @return result of the subtraction
	  */
	def -(that: T): T

	/**
	  * Multiplies this BagCounter with another BagCounter of the same type.
	  * @param that other BagCounter of the same type
	  * @return product of the two BagCounters
	  */
	def *(that: T): T

	/**
	  * Divides this BagCounter by another BagCounter of the same type.
	  * @param that other BagCounter of the same type
	  * @return result of the division
	  */
	def /(that: T): T

	/**
	  * Returns true if this BagCounter equals 0.
	  * @return true if this BagCounter equals 0.
	  */
	def equalsZero(): Boolean

	/**
	  * Returns true if this BagCounter is less than 0.
	  * @return true if this BagCounter is less than 0.
	  */
	def lessThanZero(): Boolean

	/**
	  * Inverts this BagCounter.
	  * @return this BagCounter with a different sign
	  */
	def invert(): T

	/**
	  * Rounds this BagCounter to two decimals.
	  * @return rounded BagCounter with up to two decimal values
	  */
	def round(): T

	/**
	  * Adds this BagCounter to another BagCounter of the same type.
	  * @param that other BagCounter of the same type
	  * @return sum of the two BagCounters
	  */
	def compare(that: T): Int
}

/**
  * Contains implicit conversions of data types to the type BagCounter.
  */
object BagCounter {
	/**
	  * Implicit conversion of type Integer to type BagCounter.
	  * @param x Integer to convert
	  * @return BagCounter of the Integer
	  */
	implicit def intToBagCounter(x: Int): BagCounter[Int] = new BagCounter[Int] {
		def +(y: Int): Int = x + y

		def -(y: Int): Int = x - y

		def *(y: Int): Int = x * y

		def /(y: Int): Int = x / y

		def equalsZero(): Boolean = x == 0

		def lessThanZero(): Boolean = x < 0

		def invert(): Int = x * -1

		def round(): Int = x

		def compare(y: Int): Int = x - y
	}

	/**
	  * Implicit conversion of type Double to type BagCounter.
	  * @param x Double to convert
	  * @return BagCounter of the Double
	  */
	implicit def doubleToBagCounter(x: Double): BagCounter[Double] = new BagCounter[Double] {
		def +(y: Double): Double = x + y

		def -(y: Double): Double = x - y

		def *(y: Double): Double = x * y

		def /(y: Double): Double = x / y

		def equalsZero(): Boolean = x == 0.0

		def lessThanZero(): Boolean = x < 0.0

		def invert(): Double = x * -1.0

		// TODO set precision
		def round(): Double = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

		def compare(y: Double): Int = (x - y).signum
	}
}
