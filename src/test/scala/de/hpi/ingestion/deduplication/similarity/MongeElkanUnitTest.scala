package de.hpi.ingestion.deduplication.similarity

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import org.scalatest.{FlatSpec, Matchers}

class MongeElkanUnitTest extends FlatSpec with Matchers {
	"score" should "return the MongeElkan score for given strings" in {
		val data = List(
			(Array("Peter", "Christen"), Array("Christian", "Pedro"), 0.8586111111111111),
			(Array("paul", "johnson"), Array("johson", "paule"), 0.9438888888888889)
		)
		data.foreach { case (s, t, score) =>
			MongeElkan.score(s, t) shouldEqual score
		}
	}

	it should "return the JaroWinkler score if tokens contain only one element" in {
		val data = (Array("Audi"), Array("Audio"))
		val expected = JaroWinklerMetric.compare(data._1(0), data._2(0)).getOrElse(0.0)
		MongeElkan.score(data._1, data._2) shouldEqual expected
	}

	"compare" should "return the MongeElkan score for given strings" in {
		val data = List(
			("Peter Christen", "Christian Pedro", 0.8586111111111111),
			("paul johnson", "johson paule", 0.9438888888888889)
		)
		data.foreach { case (s, t, score) =>
			MongeElkan.compare(s, t) shouldEqual score
		}
	}

	it should "be symmetric" in {
		val token = ("Audi AG", "Audi")
		val score = MongeElkan.compare(token._1, token._2)
		val symScore = MongeElkan.compare(token._2, token._1)
		score shouldEqual symScore
		score shouldEqual 1.0
	}
}
