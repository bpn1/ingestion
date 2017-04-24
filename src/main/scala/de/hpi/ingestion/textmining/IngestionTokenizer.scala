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

	/**
	  * Reverts the tokenization step. Only useful when the tokens were not stemmed and the stopwords were not filtered.
	  * @param tokens List of tokens to join
	  * @return String of the tokens joined
	  */
	def reverse(tokens: List[String]): String = tokenizer.reverse(tokens)
}

/**
  * Companion Object for IngestionTokenizer class implementing apply methods for a default tokenizer.
  */
object IngestionTokenizer {
	/**
	  * Creates default tokenizer which uses the CoreNLPTokenizer, removes stopwords and stems.
	  * @return returns tokenizer which uses the CoreNLPTokenizer, removes stopwords and stems
	  */
	def apply(): IngestionTokenizer = new IngestionTokenizer(new CoreNLPTokenizer, true, true)

	/**
	  * Creates default tokenizer which uses the CoreNLPTokenizer and takes parameters for removing stopwords and
	  * stemming.
	  * @param stopWords flag for removing stopwords
	  * @param stem flag for stemming
	  * @return returns tokenizer which uses the CoreNLPTokenizer and removes stopwords and stems according to the flags
	  */
	def apply(stopWords: Boolean, stem: Boolean): IngestionTokenizer = {
		new IngestionTokenizer(new CoreNLPTokenizer, stopWords, stem)
	}

	/**
	  * Uses default constructor to create a tokenizer with the given tokenizer which removes stopwords and stems
	  * according to the flags.
	  * @param tokenizer tokenizer to use
	  * @param stopWords flag for removing stopwords
	  * @param stem flag for stemming
	  * @return returns tokenizer which uses the CoreNLPTokenizer and removes stopwords and stems according to the flags
	  */
	def apply(tokenizer: Tokenizer, stopWords: Boolean = false, stem: Boolean = false): IngestionTokenizer = {
		new IngestionTokenizer(tokenizer, stopWords, stem)
	}
}
