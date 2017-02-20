import scala.collection.immutable.Set
import scala.collection.SetLike
import scala.collection.mutable.Builder

class Bag[A](private val counts: Map[A, Int] = Map.empty[A, Int]) extends SetLike[A, Bag[A]] with Set[A] {
	// http://stackoverflow.com/questions/15065070/implement-a-multiset-bag-as-scala-collection
	override def +(elem: A): Bag[A] = {
		val count = this.counts.getOrElse(elem, 0) + 1
		new Bag(this.counts + (elem -> count))
	}

	override def -(elem: A): Bag[A] = this.counts.get(elem) match {
		case None => this
		case Some(1) => new Bag(this.counts - elem)
		case Some(n) => new Bag(this.counts + (elem -> (n - 1)))
	}

	override def contains(elem: A): Boolean = this.counts.contains(elem)

	override def empty: Bag[A] = new Bag[A]

	override def iterator: Iterator[A] = {
		for ((elem, count) <- this.counts.iterator; _ <- 1 to count) yield elem
	}

	override def newBuilder: Builder[A, Bag[A]] = new Builder[A, Bag[A]] {
		var Bag = empty

		def +=(elem: A): this.type = {
			this.Bag += elem
			this
		}

		def clear(): Unit = this.Bag = empty

		def result(): Bag[A] = this.Bag
	}

	override def seq: Bag[A] = this
}

object Bag {
	def empty[A]: Bag[A] = new Bag[A]

	def apply[A](elem: A, elems: A*): Bag[A] = Bag.empty + elem ++ elems

	def apply[A](elems: Seq[A]): Bag[A] = Bag.empty ++ elems

	def apply[A](elem: (A, Int), elems: (A, Int)*) = new Bag((elem +: elems).toMap)

	def apply[A](elems: Map[A, Int]): Bag[A] = new Bag(elems)
}