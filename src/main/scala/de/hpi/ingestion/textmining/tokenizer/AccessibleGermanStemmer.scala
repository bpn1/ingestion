package de.hpi.ingestion.textmining.tokenizer

import org.apache.lucene.analysis.de.GermanStemmer

/**
  * The Serializable Version of the German Stemmer.
  */
class AccessibleGermanStemmer extends GermanStemmer with Serializable {
	/**
	  * Makes the stem function accessible.
	  *
	  * @param term term to be stemmed
	  * @return stemmed term
	  */
	override def stem(term: String): String = {
		super.stem(term)
	}
}
