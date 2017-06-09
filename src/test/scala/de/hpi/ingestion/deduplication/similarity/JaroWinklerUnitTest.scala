package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class JaroWinklerUnitTest extends FlatSpec with Matchers {

	"compare" should "return the JaroWinkler score for given strings" in {
		// results confirmed by https://asecuritysite.com/forensics/simstring
		val testData = List(
			("dwayne", "duane", 0.8400000000000001), // result confirmed in round terms
			("jones", "johnson", 0.8323809523809523), // result confirmed in round terms
			("fvie", "ten", 0.0))

		testData.foreach(tuple =>
			JaroWinkler.compare(tuple._1, tuple._2) shouldEqual tuple._3)
	}
}
