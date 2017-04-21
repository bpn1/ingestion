package de.hpi.ingestion.textmining

import scala.io.Source

/**
  * An advanced tokenizer that uses your tokenizer, removes stopwords if you want to and stems the tokens
  * @param tokenizer tokenizer to use
  * @param removeStopwords flag if you want to remove stopwords
  */
class IngestionTokenizer(tokenizer: Tokenizer, removeStopwords: Boolean) extends Serializable {
	val stopwordsPath = "german_stopwords.txt"
	val stopwords = Source.fromURL(getClass.getResource(s"/$stopwordsPath"))
		.getLines()
		.toSet

	/**
	  * main function to tokenize, remove and stem
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
