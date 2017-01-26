import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec
import org.apache.spark.rdd.RDD

class WikipediaLinkAnalysisTest extends FlatSpec with SharedSparkContext {
	"Grouped aliases" should "not be empty" in {
		val groupedAliases = WikipediaLinkAnalysis.groupAliasesByPageNames(ParsedWikipediaTestRDD())
		assert(groupedAliases.count > 0)
	}

	def ParsedWikipediaTestRDD() : RDD[WikipediaTextparser.ParsedWikipediaEntry] = {
		sc.parallelize(List(
			WikipediaTextparser.ParsedWikipediaEntry("Audi", "dummy text", List(
				WikipediaTextparser.Link("Ingolstadt", "Ingolstadt", 55),
				WikipediaTextparser.Link("Bayern", "Bayern", 69),
				WikipediaTextparser.Link("Automobilhersteller", "Automobilhersteller", 94)
		))))
	}
}
