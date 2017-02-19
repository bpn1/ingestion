package DataLake

/**
	* A hybrid similarity measure comparing strings corresponding to the Soft tf-idf algorithm
	*/
object SoftTFIDF extends SimilarityMeasure[String] {
	/**
		* Calculates the Soft tf-idf similarity score for two strings
		* @param s string to be compared to t
		* @param t string to be compared to s
		* @return a normalized similarity score between 1.0 and 0.0
		*/
	override def compare(s: String, t: String) = 0.0
}
