package DataLake

/**
  * Provides a method to measure the similarity of two objects
  * @tparam T the type of the objects to be compared
  */
trait SimilarityMeasure[T] extends Serializable {
	/**
	  * Calculates a similarity score for two objects
	  * @param x object to be compared to y
	  * @param y object to be compared to x
	  * @return a normalized similarity score between 1.0 and 0.0
	  */
	def compare(x: T, y: T) : Double
}
