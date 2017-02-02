package DataLake

abstract class ExactMatch[T] extends SimilarityMeasure[T] {
	override def score(x: T, y: T) = if(x == y) 1.0 else 0.0
}

object ExactMatchString extends ExactMatch[String]
object ExactMatchDouble extends ExactMatch[Double]
