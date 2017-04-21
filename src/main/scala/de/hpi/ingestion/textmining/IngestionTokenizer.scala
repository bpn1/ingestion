package de.hpi.ingestion.textmining

import scala.io.Source

/**
  * An advanced tokenizer that uses a given tokenizer, removes stopwords if flag is set and stems the tokens
  * @param tokenizer 
  * @param removeStopwords flag for removing stopwords
  */
class IngestionTokenizer(tokenizer: Tokenizer, removeStopwords: Boolean) extends Serializable {
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
		if (removeStopwords) tokens = tokens.filterNot(stopwords)
		tokens.map(stemmer.stem)
	}
}
