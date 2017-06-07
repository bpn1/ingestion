package de.hpi.ingestion.textmining.tokenizer

import de.hpi.ingestion.textmining.TestData
import org.scalatest.{FlatSpec, Matchers}

class AccessibleGermanStemmerTest extends FlatSpec with Matchers {
	"German stemmer" should "stem German words" in {
		val stemmer = new AccessibleGermanStemmer
		val stemmedWords = TestData.unstemmedGermanWordsList()
		    .map(stemmer.stem)
		stemmedWords shouldEqual TestData.stemmedGermanWordsList
	}
}
