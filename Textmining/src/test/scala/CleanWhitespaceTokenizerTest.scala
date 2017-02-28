import org.scalatest.FlatSpec

class CleanWhitespaceTokenizerTest extends FlatSpec with PrettyTester {
	"Cleaned tokenized sentences" should "contain multiple tokens" in {
		val tokenizer = new CleanWhitespaceTokenizer
		testSentences()
			.map(tokenizer.tokenize)
			.foreach(tokens => assert(tokens.length > 1))
	}

	they should "be exactly these token lists" in {
		val tokenizer = new CleanWhitespaceTokenizer
		val tokenizedSentences = testSentences()
			.map(tokenizer.tokenize)
		assert(areListsEqual(tokenizedSentences, tokenizedTestSentences()))
	}

	def testSentences(): List[String] = {
		// scalastyle:off line.size.limit
		List(
			"This is a test sentence.",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen.",
			"Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."
		)
		// scalastyle:on line.size.limit
	}

	def tokenizedTestSentences(): List[List[String]] = {
		// scalastyle:off line.size.limit
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "in", "Hessen"),
			List("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine", "Links", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen")
		)
		// scalastyle:on line.size.limit
	}
}
