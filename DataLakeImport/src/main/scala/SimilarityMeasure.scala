package DataLake

trait SimilarityMeasure[T] {
	def score(x: T, y: T) : Double
}
