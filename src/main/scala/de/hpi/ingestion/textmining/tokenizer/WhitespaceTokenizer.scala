package de.hpi.ingestion.textmining.tokenizer

/**
  * Tokenizes the given text on spaces.
  */
class WhitespaceTokenizer() extends Tokenizer {
	def tokenize(text: String): List[String] = text.split(" ").toList

	def reverse(tokens: List[String]): String = tokens.mkString(" ")
}
