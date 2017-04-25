package de.hpi.ingestion.textmining

import org.apache.lucene.analysis.de.GermanStemmer

/**
  * The Serializable Version of the German Stemmer
  */
// TODO find a thread safe solution
class AccessibleGermanStemmer extends GermanStemmer with Serializable {
	/**
	  * To make the stem function accessible.
	  * @param term term to be stemmed
	  * @return stemmed term
	  */
	override def stem(term: String): String = {
		super.stem(term)
	}
}
