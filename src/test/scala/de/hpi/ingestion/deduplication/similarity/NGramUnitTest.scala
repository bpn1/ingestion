package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class NGramUnitTest extends FlatSpec with Matchers {

	"compare" should "return the NGram score for given strings" in {
		val s1 = "nacht"
		val t1 = "night"
		val r1 = 0.6
		val score1 = NGram.compare(s1, t1)
		score1 shouldEqual r1
	}
}
