import com.holdenkarau.spark.testing.{SharedSparkContext, RDDComparisons}
import org.scalatest.FlatSpec
import org.apache.spark.rdd._
import WikiClasses._

class WikipediaLinkAnalysisTest extends FlatSpec with SharedSparkContext with RDDComparisons {
	"Grouped aliases" should "not be empty" in {
		val groupedAliases = WikipediaLinkAnalysis.groupByAliases(parsedWikipediaTestRDD())
		assert(groupedAliases.count > 0)
	}

	they should "be exactly these aliases" in {
		val groupedAliases = WikipediaLinkAnalysis.groupByAliases(parsedWikipediaTestRDD())
			    .map(alias => (alias.alias, alias.pages.toMap))
		val groupedAliasesTest = groupedAliasesTestRDD()
			.map(alias => (alias.alias, alias.pages.toMap))
		assertRDDEquals(groupedAliases, groupedAliasesTest)
	}

	"Grouped page names" should "not be empty" in {
		val groupedPageNames = WikipediaLinkAnalysis.groupByPageNames(parsedWikipediaTestRDD())
		assert(groupedPageNames.count > 0)
	}

	they should "be exactly these page names" in {
		val groupedPages = WikipediaLinkAnalysis.groupByPageNames(parsedWikipediaTestRDD())
		val groupedPagesTest = groupedPagesTestRDD()
		assertRDDEquals(groupedPages, groupedPagesTest)
	}

	"Probability that link directs to page" should "be computed correctly" in {
		val references = probabilityReferences()
		cleanedGroupedAliasesTestRDD()
			.map(link =>
				(link.alias, WikipediaLinkAnalysis.probabilityLinkDirectsToPage(link, "Bayern")))
			.collect
			.foreach { case (alias, probability) => assert(probability == references(alias)) }
	}

	"Dead links and only dead links" should "be removed" in {
		val cleanedGroupedAliases = WikipediaLinkAnalysis.removeDeadLinks(
			groupedAliasesTestRDD(),
			allPagesTestRDD())
		val cleanedGroupedAliasesTest = cleanedGroupedAliasesTestRDD()
		assertRDDEquals(cleanedGroupedAliases, cleanedGroupedAliasesTest)
	}

	"Dead pages and only dead pages" should "be removed" in {
		val cleanedGroupedPages = WikipediaLinkAnalysis.removeDeadPages(
			groupedPagesTestRDD(),
			allPagesTestRDD())
		val cleanedGroupedPagesTest = cleanedGroupedPagesTestRDD()
		assertRDDEquals(cleanedGroupedPages, cleanedGroupedPagesTest)
	}

	def parsedWikipediaTestRDD(): RDD[ParsedWikipediaEntry] = {
		sc.parallelize(List(

			ParsedWikipediaEntry("Audi", Option("dummy text"), List(
				Link("Ingolstadt", "Ingolstadt", 55),
				Link("Bayern", "Bayern", 69),
				Link("Automobilhersteller", "Automobilhersteller", 94),
				Link("Zerfall", "Zerfall (Album)", 4711),
				Link("Zerfall", "Zerfall (Soziologie)", 4711) // dead link
			))))
	}

	def groupedAliasesTestRDD(): RDD[Alias] = {
		sc.parallelize(List(
			Alias("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Alias("Bayern", Map("Bayern" -> 1).toSeq),
			Alias("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Alias("Zerfall", Map("Zerfall (Album)" -> 1, "Zerfall (Soziologie)" -> 1).toSeq)
		))
	}

	def groupedPagesTestRDD(): RDD[Page] = {
		sc.parallelize(List(
			Page("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Page("Bayern", Map("Bayern" -> 1).toSeq),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Page("Zerfall (Album)", Map("Zerfall" -> 1).toSeq),
			Page("Zerfall (Soziologie)", Map("Zerfall" -> 1).toSeq)
		))
	}

	def cleanedGroupedAliasesTestRDD(): RDD[Alias] = {
		sc.parallelize(List(
			Alias("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Alias("Bayern", Map("Bayern" -> 1).toSeq),
			Alias("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Alias("Zerfall", Map("Zerfall (Album)" -> 1).toSeq)
		))
	}

	def cleanedGroupedPagesTestRDD(): RDD[Page] = {
		sc.parallelize(List(
			Page("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Page("Bayern", Map("Bayern" -> 1).toSeq),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Page("Zerfall (Album)", Map("Zerfall" -> 1).toSeq)
		))
	}

	def probabilityReferences(): Map[String, Double] = {
		Map(
			"Ingolstadt" -> 0.0,
			"Bayern" -> 1.0,
			"Automobilhersteller" -> 0.0,
			"Zerfall" -> 0.0
		)
	}

	def allPagesTestRDD(): RDD[String] = {
		sc.parallelize(List(
			"Automobilhersteller",
			"Ingolstadt",
			"Bayern",
			"Zerfall (Album)"
		))
	}

}
