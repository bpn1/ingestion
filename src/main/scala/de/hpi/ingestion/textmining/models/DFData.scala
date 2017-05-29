package de.hpi.ingestion.textmining.models

import java.io.{BufferedReader, InputStreamReader}

import de.hpi.ingestion.textmining.{AliasTrieSearch, CosineContextComparator, DocumentFrequencyCounter}

import scala.collection.mutable

/**
  * Contains all data relevant for the idf calculation.
  * @param numDocuments maximum number of documents
  * @param defaultIdf default idf used if a term is not contained in the idf Map
  * @param idfMap Map containing the idf values for every term
  */
case class DFData(
	numDocuments: Long,
	defaultIdf: Double,
	idfMap: Map[String, Double] = Map()
) {
	val idf = (calculateIdf _).tupled

	/**
	  * Returns the idf of a given term.
	  * @param term term whose idf will be returned
	  * @return idf of the term
	  */
	def apply(term: String): Double = {
		idfMap.getOrElse(term, defaultIdf)
	}

	/**
	  * Calculates the idf for a given term and its term frequency using the {@idfMap}.
	  * @param term term whose idf will be calculated
	  * @param tf term frequency of the term
	  * @return tuple (term, idf)
	  */
	def calculateIdf(term: String, tf: Int): (String, Double) = {
		val idf = this(term)
		val tfidf = tf * idf
		(term, tfidf)
	}
}

/**
  * Companion object for the DFData case class.
  */
object DFData {

	/**
	  * Returns DFData object containing the deserialized idf Map, the default idf and the number of documents used.
	  * @param numDocuments maximum number of documents
	  * @param dfThreshold lowest possible document frequency
	  * @return DFData containing the deserialized idf Map, the default idf and the number of documents used
	  */
	def apply(numDocuments: Long, dfThreshold: Int): DFData = {
		val defaultIdf = calculateIdf(defaultDF(dfThreshold), numDocuments)
		val idfMap = inverseDocumentFrequencies(numDocuments)
		new DFData(numDocuments, defaultIdf, idfMap)
	}

	/**
	  * Returns default value for the Document Frequency used in the idf calculation.
	  * @param threshold least significant Document Frequency
	  * @return default Document Frequency used for idf calculation
	  */
	def defaultDF(threshold: Int = DocumentFrequencyCounter.leastSignificantDocumentFrequency): Int = {
		threshold - 1
	}

	/**
	  * Calculates the inverse document frequency (idf) using the logarithm base 10 for a given Document Frequency and
	  * the number of documents used to calculate it.
	  *
	  * @param df      document frequency to use
	  * @param numDocs number of documents used to compute the document frequency
	  * @return calculated inverse document frequency
	  */
	def calculateIdf(df: Int, numDocs: Long): Double = {
		Math.log10(numDocs.toDouble / df.toDouble)
	}

	/**
	  * Reads the document frequencies from the hdfs, calculates the inverse document frequencies and returns them
	  * as Map.
	  * @param numDocs number of documents used to create the document frequency data
	  * @return Map of every term and its inverse document frequency
	  */
	def inverseDocumentFrequencies(numDocs: Long): Map[String, Double] = {
		val docfreqStream = AliasTrieSearch.trieStreamFunction(CosineContextComparator.settings("dfFile"))
		val reader = new BufferedReader(new InputStreamReader(docfreqStream, "UTF-8"))
		val idfMap = mutable.Map[String, Double]()
		var line = reader.readLine()
		while(line != null) {
			val Array(count, token) = line.split("\t", 2)
			idfMap(token) = calculateIdf(count.toInt, numDocs)
			line = reader.readLine()
		}
		idfMap.toMap
	}
}
