package de.hpi.ingestion.textmining

import scala.io.Source

class IngestionTokenizer(tokenizer: Tokenizer, removeStopwords: Boolean) extends Serializable {
	val stopwordsPath = "german_stopwords.txt"
	val stopwords = Source.fromURL(getClass.getResource(s"/$stopwordsPath"))
		.getLines()
		.toSet
	def process(text: String): List[String] = {
		val stemmer = new AccessibleGermanStemmer
		var tokens = tokenizer.tokenize(text)
		if (removeStopwords) tokens = tokens.filterNot(stopwords)
		tokens.map(stemmer.stem)
	}
}
