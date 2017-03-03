package DataLake

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import org.scalatest.{FlatSpec, Matchers}

class MongeElkanUnitTest extends FlatSpec with Matchers {
	"maxSim" should "return the score with the highest similarity" in {
		val maximum = MongeElkan.maxSim("henka", List("henkan", "123", "xyz"))
		maximum shouldEqual JaroWinklerMetric.compare("henka", "henkan").get
	}

	"compare" should "return the MongeElkan score for given strings" in {
		val score = MongeElkan.compare("henka", "henkan")
		score shouldEqual JaroWinklerMetric.compare("henka", "henkan").get
	}
}
