package de.hpi.ingestion.textmining.models

import scala.math.BigDecimal
import scala.language.implicitConversions

trait BagCounter[T] extends Ordered[T] {
	def +(that: T): T

	def -(that: T): T

	def *(that: T): T

	def /(that: T): T

	def equalsZero(): Boolean

	def lessThanZero(): Boolean

	def invert(): T

	def round(): T

	def compare(that: T): Int
}

object BagCounter {
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
