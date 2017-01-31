import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec
import org.apache.spark.rdd._

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
		val groupedAliases = WikipediaLinkAnalysis.groupAliasesByPageNames(parsedWikipediaTestRDD())
		val groupedAliasesTest = groupedAliasesTestRDD()
		assert(areRDDsEqual(groupedAliases, groupedAliasesTest))
	}

	"Probability that link directs to page" should "be computed correctly" in {
		val references = probabilityReferences()
		val probabilities = WikipediaLinkAnalysis.groupAliasesByPageNames(parsedWikipediaTestRDD())
			.map(link => (link.alias, WikipediaLinkAnalysis.probabilityLinkDirectsToPage(link, "Bayern")))
			.collect
			.foreach { case (alias, probability) => assert(probability == references(alias)) }
	}

	def areRDDsEqual(left: RDD[WikipediaLinkAnalysis.Link], right: RDD[WikipediaLinkAnalysis.Link]): Boolean = {
		val sizeLeft = left.count
		val sizeRight = right.count
		if (sizeLeft != sizeRight) return false
		val rdd1 = left
			.keyBy(_.alias)
			.join(right.keyBy(_.alias))
		if (rdd1.count != sizeLeft) return false
		rdd1
			.collect
			.foreach { case (alias, (leftLink, rightLink)) => if (leftLink.pages != rightLink.pages) return false }
		true
	}

	def parsedWikipediaTestRDD(): RDD[WikipediaTextparser.ParsedWikipediaEntry] = {
		sc.parallelize(List(
			new WikipediaTextparser.ParsedWikipediaEntry("Audi", Option("dummy text"), List(
				WikipediaTextparser.Link("Ingolstadt", "Ingolstadt", 55),
				WikipediaTextparser.Link("Bayern", "Bayern", 69),
				WikipediaTextparser.Link("Automobilhersteller", "Automobilhersteller", 94)
			))))
	}

	def groupedAliasesTestRDD(): RDD[WikipediaLinkAnalysis.Link] = {
		sc.parallelize(List(
			WikipediaLinkAnalysis.Link("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Bayern", Map("Bayern" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq)
		))
	}

	def probabilityReferences(): Map[String, Double] = {
		Map(
			"Ingolstadt" -> 0.0,
			"Bayern" -> 1.0,
			"Automobilhersteller" -> 0.0
		)
	}
}
