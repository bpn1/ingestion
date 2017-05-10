package de.hpi.ingestion.textmining.tokenizer

import de.hpi.ingestion.textmining.models.OffsetToken

/**
  * Trait used for textmining tokenizers. Declares methods to tokenize a text and create a text from a list of tokens.
  */
trait Tokenizer extends Serializable {
	/**
	  * Tokenizes a text into a list of tokens that represent a word or a punctuation character.
	  *
	  * @param text untokenized text
	  * @return tokens
	  */
	def tokenize(text: String): List[String]

	/**
	  * Tokenizes a text into a list of tokens with their begin character offset.
	  *
	  * @param text untokenized text
	  * @return tokens with offsets
	  */
	def tokenizeWithOffsets(text: String): List[OffsetToken] = Nil

	/**
	  * Retrieves the original text from a list of tokens.
	  *
	  * @param tokens tokens
	  * @return original text
	  */
	def reverse(tokens: List[String]): String
}
