package de.hpi.ingestion.textmining.tokenizer

/**
  * Uses the CoreNLPTokenizer but removes control characters from the resulting tokens.
  */
class CleanCoreNLPTokenizer() extends CoreNLPTokenizer {
	override def tokenize(text: String): List[String] = {
		val tokens = super.tokenize(text)
		val badTokens = Set[String](".", "!", "?", ",", ";", ":")
		tokens.filter(token => !badTokens.contains(token))
	}
}
