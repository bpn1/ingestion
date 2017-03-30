import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.mutable

class WikipediaRedirectResolverTest extends FlatSpec with SharedSparkContext with Matchers {
	"All redirects in an entry" should "be resolved" in {
		val dict = mutable.Map[String, String]() ++ TestData.testRedirectDict
		val links= TestData.testLinksWithRedirects
		val resolvedLinks = links.map(
			WikipediaRedirectResolver.cleanLinkOfRedirects(_, dict)
		)
		resolvedLinks shouldBe TestData.testLinksWithResolvedRedirects
	}

	"All looped or bad redirect pages" should "be filtered" in {
		val entries = TestData.testRedirectCornerCaseEntries
		entries.filter { case (entry) =>
			WikipediaRedirectResolver.isValidRedirect(entry.textlinks.head, entry.title)
		} shouldBe empty
	}
}
