package de.hpi.ingestion.textmining

import scala.io.Source

/**
  * An advanced tokenizer that uses a given tokenizer, removes stop words if flag is set and stems the tokens
  * @param tokenizer Tokenizer to use
  * @param removeStopwords flag for removing stop words
  * @param stem flag for stemming
  */
class IngestionTokenizer(
	tokenizer: Tokenizer,
	removeStopwords: Boolean = false,
	stem: Boolean = false
) extends Serializable {
	val stopwordsPath = "german_stopwords.txt"
	val stopwords = Source.fromURL(getClass.getResource(s"/$stopwordsPath"))
		.getLines()
		.toSet

	/**
	  * Tokenizes a text, removes stop words and stems the tokens.
	  * @param text text to be processed into tokens
	  * @return a List of all words remaining and stemmed
	  */
	def process(text: String): List[String] = {
		val stemmer = new AccessibleGermanStemmer
		var tokens = tokenizer.tokenize(text)
		if(removeStopwords) tokens = tokens.filterNot(stopwords)
		if(stem) tokens = tokens.map(stemmer.stem)
		tokens
	}

	/**
	  * Removes stopwords and stems a list of tokens.
	  * @param tokens List of tokens to be processed
	  * @return List of stemmed words which were not stopwords
	  */
	def process(tokens: List[String]): List[String] = {
		val stemmer = new AccessibleGermanStemmer
		var newTokens = tokens
		if(removeStopwords) newTokens = newTokens.filterNot(stopwords)
		if(stem) newTokens = newTokens.map(stemmer.stem)
		newTokens
	}

	/**
	  * Tokenizes a text.
	  * @param text text to be tokenized
	  * @return List of tokens
	  */
	def onlyTokenize(text: String): List[String] = {
		tokenizer.tokenize(text)
	}
}
