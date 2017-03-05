import scala.collection.mutable.Builder
import scala.language.implicitConversions

trait BagCounter[T] {
	def +(that: T): T
	def -(that: T): T
	def *(that: T): T
	def /(that: T): T
	def floor(): Int
}

object BagCounter {
	implicit def intToBagCounter(x : Int): BagCounter[Int] = new BagCounter[Int] {
		def +(y : Int): Int = x + y
		def -(y : Int): Int = x - y
		def *(y : Int): Int = x * y
		def /(y : Int): Int = x / y
		def floor(): Int = x
	}

	implicit def doubleToBagCounter(x : Double): BagCounter[Double] = new BagCounter[Double] {
		def +(y : Double): Double = x + y
		def -(y : Double): Double = x - y
		def *(y : Double): Double = x * y
		def /(y : Double): Double = x / y
		def floor(): Int = x.toInt
	}
}

class Bag[A, B <% BagCounter[B]](
	private val counts: Map[A, B] = Map.empty[A, B]) extends SetLike[A, Bag[A, B]] with Set[A]
{

	// http://stackoverflow.com/questions/15065070/implement-a-multiset-bag-as-scala-collection

	override def +(elem: A): Bag[A, B] = {
		val count = this.counts.getOrElse(elem, 0.asInstanceOf[B]) + 1.asInstanceOf[B]
		new Bag(this.counts + (elem -> count))
	}

	override def -(elem: A): Bag[A, B] = this.counts.get(elem) match {
		case None => this
		case Some(1) => new Bag(this.counts - elem)
		case Some(n) => new Bag(this.counts + (elem -> (n - 1.asInstanceOf[B])))
	}

	def equals(bag: Bag[A]): Boolean = {
		this.counts == bag.counts
	}

	override def empty: Bag[A, B] = new Bag[A, B]

	override def iterator: Iterator[A] = {
		for ((elem, count) <- this.counts.iterator; _ <- 1 to count.floor) yield elem
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
}

object Bag {
	def empty[A, B](implicit bToBagCounter: B => BagCounter[B]): Bag[A, B] = new Bag[A, B]

	def apply[A, B](elem: A, elems: A*)(implicit bToBagCounter: B => BagCounter[B]): Bag[A, B] = new Bag[A, B] + elem ++ elems

	def apply[A, B](elems: Seq[A])(implicit bToBagCounter: B => BagCounter[B]): Bag[A, B] = new Bag[A, B] ++ elems

	def apply[A, B](elem: (A, B), elems: (A, B)*)(implicit bToBagCounter: B => BagCounter[B]) = new Bag((elem +: elems).toMap)

	def apply[A, B](elems: Map[A, B])(implicit bToBagCounter: B => BagCounter[B]): Bag[A, B] = new Bag(elems)
}
