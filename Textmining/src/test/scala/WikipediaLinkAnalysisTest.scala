import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec
import org.apache.spark.rdd.RDD

class WikipediaLinkAnalysisTest extends FlatSpec with SharedSparkContext {
	"Grouped aliases" should "not be empty" in {
		val groupedAliases = WikipediaLinkAnalysis.groupAliasesByPageNames(parsedWikipediaTestRDD())
		assert(groupedAliases.count > 0)
	}

	"Grouped page names" should "not be empty" in {
		val groupedPageNames = WikipediaLinkAnalysis.groupPageNamesByAliases(parsedWikipediaTestRDD())
		assert(groupedPageNames.count > 0)
	}

	"Grouped aliases" should "be exactly these aliases" in {

	}

	def parsedWikipediaTestRDD() : RDD[WikipediaTextparser.ParsedWikipediaEntry] = {
		sc.parallelize(List(
			new WikipediaTextparser.ParsedWikipediaEntry("Audi", Option("dummy text"), List(
				WikipediaTextparser.Link("Ingolstadt", "Ingolstadt", 55),
				WikipediaTextparser.Link("Bayern", "Bayern", 69),
				WikipediaTextparser.Link("Automobilhersteller", "Automobilhersteller", 94)
		))))
	}

	def groupedAliasesTestRDD() : RDD[WikipediaLinkAnalysis.Link] = {
		sc.parallelize(List(
			new WikipediaLinkAnalysis.Link()
		))
	}
}
