import org.scalatest.FlatSpec

class CleanWhitespaceTokenizerTest extends FlatSpec with PrettyTester {
	"Cleaned tokenized sentence" should "contain multiple tokens" in {
		val tokenizer = new CleanWhitespaceTokenizer
		testSentences()
			.map(tokenizer.tokenize)
			.foreach(tokens => assert(tokens.length > 1))
	}

	"Cleaned tokenized sentences" should "be exactly these token lists" in {
		val tokenizer = new CleanWhitespaceTokenizer
		val tokenizedSentences = testSentences()
			.map(tokenizer.tokenize)
		assert(areListsEqual(tokenizedSentences, tokenizedTestSentences()))
	}

	def testSentences(): List[String] = {
		List(
			"This is a test sentence.",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen.",
			"Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."
		)
	}

	def tokenizedTestSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "in", "Hessen"),
			List("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine", "Links", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen")
		)
	}
}
