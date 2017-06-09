package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class SimilarityMeasureTest extends FlatSpec with Matchers {

	"Similarity Measure" should "be returned given its name" in {
		SimilarityMeasure.get[String]("ExactMatchString") shouldEqual ExactMatchString
		SimilarityMeasure.get[Double]("ExactMatchDouble") shouldEqual ExactMatchDouble
		SimilarityMeasure.get[String]("MongeElkan") shouldEqual MongeElkan
		SimilarityMeasure.get[String]("Jaccard") shouldEqual Jaccard
		SimilarityMeasure.get[String]("DiceSorensen") shouldEqual DiceSorensen
		SimilarityMeasure.get[String]("Jaro") shouldEqual Jaro
		SimilarityMeasure.get[String]("JaroWinkler") shouldEqual JaroWinkler
		SimilarityMeasure.get[String]("N-Gram") shouldEqual NGram
		SimilarityMeasure.get[String]("Overlap") shouldEqual Overlap
		SimilarityMeasure.get[String]("EuclidianDistance") shouldEqual EuclidianDistance
		SimilarityMeasure.get[String]("RelativeNumbersSimilarity") shouldEqual RelativeNumbersSimilarity
		SimilarityMeasure.get[String]("Not existing") shouldEqual ExactMatchString
	}
}
