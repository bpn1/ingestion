import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class TagEntitiesTest extends FlatSpec with SharedSparkContext with Matchers {
	"Shorter paths" should "be added" in {
		//addShorterPaths
		assert(false)
	}

	"Subclass map" should "be built" in {
		//buildSubclassMap
		assert(false)
	}

	"Wikidata entity" should "be properly translated into SublcassEntry" in {
		//translateToSubclassEntry
		assert(false)
	}

	"Instance of property" should "be checked correctly" in {
		//isInstanceOf
		assert(false)
	}

	it should "be updated correctly" in {
		//updateInstanceOfProperty
		assert(false)
	}
}
