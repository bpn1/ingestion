package de.hpi.ingestion.textmining

import scala.io.Source

class CleaningStemmingTokenizer(tokenizer: Tokenizer, stopwords: Set[String]) extends Serializable {
	def tokenizeRemoveStopwordsStem(text: String): List[String] = {
		val stemmer = new AccessibleGermanStemmer
		tokenizer.tokenize(text)
			.filter(word => !stopwords.contains(word))
			.map(word => stemmer.stem(word))
	}
}
