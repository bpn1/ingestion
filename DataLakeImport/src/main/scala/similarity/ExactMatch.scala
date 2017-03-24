package DataLake

/**
  * An abstract binary similarity measure for exact matching
  * @tparam T the type of data to be compared
  */
abstract class ExactMatch[T] extends SimilarityMeasure[T] {
	/**
	  * Comparing the given objects on exact matching
	  * @param x object to be compared to y
	  * @param y object to be compared to x
	  * @param u has no specific use in here
	  * @return 1.0 if given objects match exactly, 0.0 otherwise
	  */
	override def compare(x: T, y: T, u: Int = 1) = if(x == y) 1.0 else 0.0
}

/**
  * A specific exact match similarity measure comparing strings
  */
object ExactMatchString extends ExactMatch[String]

/**
  * A specific exact match similarity measure comparing Doubles
  */
object ExactMatchDouble extends ExactMatch[Double]

