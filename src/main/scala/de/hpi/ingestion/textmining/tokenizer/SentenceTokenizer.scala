package de.hpi.ingestion.textmining.tokenizer

import edu.stanford.nlp.simple.GermanDocument
import scala.collection.JavaConverters._

/**
  * Uses the CoreNLP Simple German API to tokenize the given text into sentences.
  */
class SentenceTokenizer() extends CoreNLPTokenizer {
	override def tokenize(txt: String) = {
		new GermanDocument(txt)
			.sentences
			.asScala
			.toList
			.map(_.text())
	}
}
