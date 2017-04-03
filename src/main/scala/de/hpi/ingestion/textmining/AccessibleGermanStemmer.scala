package de.hpi.ingestion.textmining

import org.apache.lucene.analysis.de.GermanStemmer

class AccessibleGermanStemmer extends GermanStemmer {
	override def stem(term: String): String = {
		super.stem(term)
	}
}
