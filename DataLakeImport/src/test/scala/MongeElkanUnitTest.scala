package DataLake

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import org.scalatest.FlatSpec

class MongeElkanUnitTest extends FlatSpec {
	"maxSim" should "return the score with the highest similarity" in {
		val maximum = MongeElkan.maxSim("henka", List("henkan", "123", "xyz"))
		assert(maximum === JaroWinklerMetric.compare("henka", "henkan").get)
	}

	"compare" should "return the MongeElkan score for given strings" in {
		val score = MongeElkan.compare("henka", "henkan")
		assert(score === JaroWinklerMetric.compare("henka", "henkan").get)
	}
}
