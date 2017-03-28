import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import WikiClasses._
import scala.collection.mutable

class WikipediaRedirectResolverTest extends FlatSpec with SharedSparkContext with Matchers {
	"All redirects in an entry" should "be resolved" in {
		val dict = mutable.Map("Postbank-Hochhaus Berlin" -> "Postbank-Hochhaus (Berlin)")
		val link = Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus Berlin", 10)
		val resolvedLink = WikipediaRedirectResolver.cleanLinkOfRedirects(link, dict)
		resolvedLink.page shouldBe "Postbank-Hochhaus (Berlin)"
	}

	"All redirect pages" should "not have looped links" in {
		val entry = ParsedWikipediaEntry(
			"?",
			Option("#WEITERLEITUNG [[?]]"),
			List[Link](Link("?", "?", 0)))
		WikipediaRedirectResolver.isValidRedirect(entry.textlinks.head, entry.title) shouldBe false
	}
}
