package de.hpi.ingestion.textmining

import org.scalatest.{FlatSpec, Matchers}

class AccessibleGermanStemmerTest extends FlatSpec with Matchers {
	"German stemmer" should "stem German words" in {
		val stemmer = new AccessibleGermanStemmer
		val stemmedWords = TestData.unstemmedGermanWordsTestList()
		    .map(stemmer.stem)
		stemmedWords shouldEqual TestData.stemmedGermanWordsTestList
	}
}
