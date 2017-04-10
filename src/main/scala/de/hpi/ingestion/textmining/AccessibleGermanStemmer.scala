package de.hpi.ingestion.textmining

import org.apache.lucene.analysis.de.GermanStemmer

// TODO find a thread safe solution
class AccessibleGermanStemmer extends GermanStemmer with Serializable {
	override def stem(term: String): String = {
		super.stem(term)
	}
}
