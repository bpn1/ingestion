package DataLake

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import org.scalatest.FlatSpec

class MongeElkanUnitTest extends FlatSpec {

	"max" should "return 0.0 if given list is empty" in {
			val maximum = MongeElkan.max(List())
			assert(maximum === 0.0)
	}
	it should "return the maximum of a given list" in {
		val maximum = MongeElkan.max(List(12.2, 2.5, 34.8, 11.0, 0.5))
		assert(maximum === 34.8)
	}

	"maxSim" should "return the score with the highest similarity" in {
		val maximum = MongeElkan.maxSim("henka", List("henkan", "123", "xyz"))
		assert(maximum === JaroWinklerMetric.compare("henka", "henkan").get)
	}

	"score" should "return the MongeElkan score for given strings" in {
		val score = MongeElkan.compare("henka", "henkan")
		assert(score === JaroWinklerMetric.compare("henka", "henkan").get)
	}
}
