package de.hpi.ingestion.textmining.tokenizer
import de.hpi.ingestion.textmining.models.OffsetToken

/**
  * Uses the CoreNLPTokenizer but removes control characters from the resulting tokens.
  */
class CleanCoreNLPTokenizer() extends CoreNLPTokenizer {
	val badTokens = Set(".", "!", "?", ",", ";", ":")
	override def tokenize(text: String): List[String] = {
		super
			.tokenize(text)
			.filterNot(badTokens)
	}

	override def tokenizeWithOffsets(text: String): List[OffsetToken] = {
		super
			.tokenizeWithOffsets(text)
			.filterNot(token => badTokens.contains(token.token))
	}
}
