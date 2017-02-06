import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

class WikipediaAliasCounterTest extends FlatSpec with SharedSparkContext {
	"Counted Aliases" should "keep the right alias name" in {
		cleanedGroupedAliasesTestRDD()
		    .map(link => (WikipediaAliasCounter.countAliasOccurences(link.alias), link.alias))
		    .collect
		    .foreach{case (aliasCounter, originalAlias) => assert(aliasCounter.alias == originalAlias)}
	}

	def cleanedGroupedAliasesTestRDD(): RDD[WikipediaLinkAnalysis.Link] = {
		sc.parallelize(List(
			WikipediaLinkAnalysis.Link("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Bayern", Map("Bayern" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Zerfall", Map("Zerfall (Album)" -> 1).toSeq)
		))
	}
}
