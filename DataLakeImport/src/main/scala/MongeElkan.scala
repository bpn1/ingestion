package DataLake

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric

object MongeElkan extends Classifier {

	def max_acc(list: List[Double], acc: Double) : Double = list match {
		case Nil => acc
		case x::xs => if (x > acc) max_acc(xs, x) else max_acc(xs, acc)
	}

	def max(list: List[Double]) : Double = max_acc(list, 0.0)

	def maxSim(token: String, list: List[String]) : Double = {
		max(list.map(x => JaroWinklerMetric.compare(token, x).getOrElse(0.0)))
	}

	def score(s: String, t: String) : Double = {
		val x = s.split(" ").toList
		val y = t.split(" ").toList
		val sum = x
		  .map(maxSim(_, y))
		  .foldLeft(0.0)((b, a) => b+a)
		sum / y.length
	}

	override def execute(subject1: Subject, subject2: Subject) : Double = {
		score(subject1.name.get, subject2.name.get)
	}
}
