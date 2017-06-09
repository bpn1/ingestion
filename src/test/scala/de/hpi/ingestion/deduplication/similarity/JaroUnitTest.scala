package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class JaroUnitTest extends FlatSpec with Matchers {

	"compare" should "return the Jaro score for given strings" in {
		// results confirmed by https://asecuritysite.com/forensics/simstring
		val testData = List(
			("dwayne", "duane", 0.8222222222222223), // result confirmed in round terms
			("jones", "johnson", 0.7904761904761904), // result confirmed in round terms
			("fvie", "ten", 0.0))

		testData.foreach(tuple =>
			Jaro.compare(tuple._1, tuple._2) shouldEqual tuple._3)
	}
}
