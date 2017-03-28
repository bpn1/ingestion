import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import WikiClasses._
import scala.collection.mutable

class WikipediaRedirectResolverTest extends FlatSpec with SharedSparkContext with Matchers {
	it should "resolve Redirects" in {
		val dict: mutable.Map[String, String] = mutable.Map("Postbank-Hochhaus Berlin" -> "Postbank-Hochhaus (Berlin)")
		val link: Link = Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus Berlin", 10)
		val resolvedLink = WikipediaRedirectResolver.cleanLinkOfRedirects(link, dict)
		resolvedLink.page shouldBe "Postbank-Hochhaus (Berlin)"
	}

	it should "filter pages that are not Redirect Pages or are wrong" in {
		val entry = ParsedWikipediaEntry("?", Option("#WEITERLEITUNG [[?]]"), List[Link](Link("?", "?", 0)))
		WikipediaRedirectResolver.checkRedirectPage(entry.links.head, entry.title) shouldBe false
	}
}