import org.scalatest.FlatSpec
import org.scalatest.Matchers

class AccessibleGermanStemmerTest extends FlatSpec with Matchers {
	"German stemmer" should "stem German words" in {
		val stemmer = new AccessibleGermanStemmer
		val stemmedWords = unstemmedGermanWordsTestList()
		    .map(stemmer.stem)
		stemmedWords shouldEqual stemmedGermanWordsTestList
	}

	def unstemmedGermanWordsTestList(): List[String] = {
		List("Es", "Keine", "Kein", "Keiner", "Ist", "keine", "keiner", "ist", "Streitberg", "Braunschweig",
			"Deutschland", "Baum", "brauchen", "suchen", "könnte")
	}

	def stemmedGermanWordsTestList(): List[String] = {
		List("es", "kein", "kein", "kein", "ist", "kein", "kein", "ist", "streitberg", "braunschweig",
			"deutschla", "baum", "brauch", "such", "konn")
	}
}
