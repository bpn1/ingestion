package de.hpi.ingestion.textmining.models

case class DFData(
	numDocuments: Long,
	defaultIdf: Double,
	idfMap: Map[String, Double] = Map()
) {
	def apply(term: String): Double = {
		idfMap.getOrElse(term, defaultIdf)
	}

	def calculateIdf(term: String, tf: Int): (String, Double) = {
		val idf = this(term)
		val tfidf = tf * idf
		(term, tfidf)
	}
}
