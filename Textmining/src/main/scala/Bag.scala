import scala.collection.mutable.Builder

class Bag[A](private val counts: Map[A, Double] = Map.empty[A, Double]) {
	// inspired by http://stackoverflow.com/questions/15065070/implement-a-multiset-bag-as-scala-collection

	def +(elem: A): Bag[A] = {
		val count = this.counts.getOrElse(elem, 0.0) + 1.0
		new Bag(this.counts + (elem -> count))
	}

	//	def ++(elems: A*): Bag[A] = {
	//		this
	//	}

	def -(elem: A): Bag[A] = this.counts.get(elem) match {
		case None => this
		case Some(1) => new Bag(this.counts - elem)
		case Some(n) => new Bag(this.counts + (elem -> (n - 1)))
	}

	def equals(bag: Bag[A]): Boolean = {
		this.counts == bag.counts
	}

	def contains(elem: A): Boolean = this.counts.contains(elem)

	def empty: Bag[A] = new Bag[A]

	def newBuilder: Builder[A, Bag[A]] = new Builder[A, Bag[A]] {
		var Bag = empty

		def +=(elem: A): this.type = {
			this.Bag += elem
			this
		}

		def clear(): Unit = this.Bag = empty

		def result(): Bag[A] = this.Bag
	}

	def seq: Bag[A] = this
}

object Bag {
	def empty[A]: Bag[A] = new Bag[A]

	//	def apply[A](elem: A, elems: A*): Bag[A] = Bag.empty + elem ++ elems
	//
	//	def apply[A](elems: Seq[A]): Bag[A] = Bag.empty ++ elems

	def apply[A](elem: (A, Double), elems: (A, Double)*) = new Bag((elem +: elems).toMap)

	def apply[A](elems: Map[A, Double]): Bag[A] = new Bag(elems)
}
