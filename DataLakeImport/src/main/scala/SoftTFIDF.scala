package DataLake

object SoftTFIDF extends SimilarityMeasure[String] {
	override def score(s: String, t: String) = 0.0
}
