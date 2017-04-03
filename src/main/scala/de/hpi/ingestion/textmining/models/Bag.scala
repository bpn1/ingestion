package de.hpi.ingestion.textmining.models

import scala.collection.immutable.Set
import scala.collection.SetLike
import scala.collection.mutable.Builder
import scala.language.implicitConversions
import scala.reflect.{ClassTag, classTag}
import scala.math.BigDecimal

trait BagCounter[T] extends Ordered[T]{
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
	implicit def intToBagCounter(x : Int): BagCounter[Int] = new BagCounter[Int] {
		def +(y : Int): Int = x + y
		def -(y : Int): Int = x - y
		def *(y : Int): Int = x * y
		def /(y : Int): Int = x / y
		def equalsZero(): Boolean = x == 0
		def lessThanZero(): Boolean = x < 0
		def invert(): Int = x * -1
		def round(): Int = x
		def compare(y: Int): Int = x - y
	}

	implicit def doubleToBagCounter(x : Double): BagCounter[Double] = new BagCounter[Double] {
		def +(y : Double): Double = x + y
		def -(y : Double): Double = x - y
		def *(y : Double): Double = x * y
		def /(y : Double): Double = x / y
		def equalsZero(): Boolean = x == 0.0
		def lessThanZero(): Boolean = x < 0.0
		def invert(): Double = x * -1.0
		// TODO set precision
		def round(): Double = BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
		def compare(y: Double): Int = (x - y).signum
	}
}

class Bag[A, B <% BagCounter[B] : ClassTag](
	private val counts: Map[A, B] = Map.empty[A, B]) extends SetLike[A, Bag[A, B]] with Set[A]
{
	// http://stackoverflow.com/questions/15065070/implement-a-multiset-bag-as-scala-collection

	def getCounts(): Map[A, B] = counts

	override def +(elem: A): Bag[A, B] = {
		val count = this.counts.get(elem) match {
			case Some(x) => {
				x match {
					case t: Int => (t + 1).asInstanceOf[B]
					case t: Double => (t + 1.0).asInstanceOf[B]
				}
			}
			case None => {
				if(classTag[B] == classTag[Int] || classTag[B] == classTag[Integer]) {
					1.asInstanceOf[B]
				} else {
					1.0.asInstanceOf[B]
				}
			}
		}
		new Bag(this.counts + (elem -> count))
	}

	def +(elem: A, times: B): Bag[A, B] = {
		if(times.lessThanZero()) {
			return this - (elem, times.invert())
		}
		if(times.equalsZero()) {
			return this
		}
		val count = this.counts.get(elem) match {
			case Some(x) => (x + times).round
			case None => times
		}
		new Bag(this.counts + (elem -> count))
	}

	def ++(that: Bag[A, B]): Bag[A, B] = {
		var result = this
		for((elem: A @unchecked, count: B) <- that.getCounts()) {
			result = result + (elem, count)
		}
		result
	}

	override def -(elem: A): Bag[A, B] = this.counts.get(elem) match {
		case None => this
		case Some(x) => {
			x match {
				case t: Int => {
					if(t == 1) {
						new Bag(this.counts - elem)
					} else {
						new Bag(this.counts + (elem -> (t - 1).asInstanceOf[B]))
					}
				}
				case t: Double => {
					val newCount = t - 1.0
					if(newCount <= 0.0) {
						new Bag(this.counts - elem)
					} else {
						new Bag(this.counts + (elem -> (t - 1.0).asInstanceOf[B]))
					}
				}
			}
		}
	}

	def -(elem: A, times: B): Bag[A, B] = {
		if(times.lessThanZero) {
			return this + (elem, times.invert())
		}
		if(times.equalsZero) {
			return this
		}
		this.counts.get(elem) match {
			case None => this
			case Some(x) => {
				val newCount = (x - times).round
				if(newCount.lessThanZero()) {
					new Bag(this.counts - elem)
				} else {
					new Bag(this.counts + (elem -> newCount))
				}
			}
		}
	}

	def --(that: Bag[A, B]): Bag[A, B] = {
		var result = this
		for((elem: A @unchecked, count: B) <- that.getCounts()) {
			result = result - (elem, count)
		}
		result
	}

	override def contains(elem: A): Boolean = this.counts.contains(elem)

	override def empty: Bag[A, B] = new Bag[A, B]

	def elementSize(count: B): Int = {
		if(classTag[B] == classTag[Double]) {
			count.asInstanceOf[Double].intValue()
		} else {
			count.asInstanceOf[Int]
		}
	}

	override def iterator: Iterator[A] = {
		for((elem, count) <- this.counts.iterator; _ <- 1 to elementSize(count)) yield elem
	}

	override def newBuilder: Builder[A, Bag[A, B]] = new Builder[A, Bag[A, B]] {
		var Bag = empty

		def +=(elem: A): this.type = {
			this.Bag += elem
			this
		}

		def clear(): Unit = this.Bag = empty

		def result(): Bag[A, B] = this.Bag
	}

	override def seq: Bag[A, B] = this

	override def equals(that: Any): Boolean = {
		this.counts == that.asInstanceOf[Bag[A, B]].getCounts()
	}

	override def hashCode(): Int = this.counts.hashCode

	override def toString(): String = this.counts.toString.replaceAll("Map", "Bag")

	def normalise(): Bag[A, Double] = {
		var result = new Bag[A, Double]
		val maxCount = this.counts.maxBy(_._2)._2 match {
			case x: Int => x.doubleValue()
			case x: Double => x
		}
		for((elem, count) <- this.counts) {
			val newValue = (count match {
				case x: Int => x.doubleValue()
				case x: Double => x
			}) / maxCount
			result = result + (elem, newValue)
		}
		result
	}

}

object Bag {
	def empty[A, B : ClassTag]
		(implicit bToBagCounter: B => BagCounter[B])
	: Bag[A, B] =
	{
		new Bag[A, B]
	}

	def apply[A, B : ClassTag]
		(elem: A, elems: A*)
		(implicit bToBagCounter: B => BagCounter[B])
	: Bag[A, B] =
	{
		new Bag[A, B] + elem ++ elems
	}

	def apply[A, B : ClassTag]
		(elems: Seq[A])
			(implicit bToBagCounter: B => BagCounter[B])
	: Bag[A, B] =
	{
		new Bag[A, B] ++ elems
	}

	def apply[A, B : ClassTag]
		(elem: (A, B), elems: (A, B)*)
		(implicit bToBagCounter: B => BagCounter[B])
	: Bag[A, B] =
	{
		new Bag((elem +: elems).toMap)
	}

	def apply[A, B : ClassTag]
		(elems: Map[A, B])
		(implicit bToBagCounter: B => BagCounter[B])
	: Bag[A, B] =
	{
		new Bag(elems)
	}
}
