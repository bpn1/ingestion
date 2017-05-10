package de.hpi.ingestion.textmining.tokenizer

import de.hpi.ingestion.textmining.models.OffsetToken
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, TokenizerAnnotator}
import edu.stanford.nlp.simple.GermanDocument
import scala.collection.JavaConverters._

/**
  * Uses the CoreNLP Simple German API to tokenize the given text.
  */
class CoreNLPTokenizer() extends Tokenizer {
	def tokenize(text: String): List[String] = {
		new GermanDocument(text)
			.sentences
			.asScala
			.toList
			.flatMap(_.words.asScala.toList)
	}

	override def tokenizeWithOffsets(text: String): List[OffsetToken] = {
		val annotation = new Annotation(text)
		val tokens = new TokenizerAnnotator(true, "German")
			.annotate(annotation)
		annotation
			.get(classOf[CoreAnnotations.TokensAnnotation])
			.asScala
			.toList
			.map { annotatedToken =>
				val token = annotatedToken.get(classOf[CoreAnnotations.ValueAnnotation])
				val beginOffset = annotatedToken.get(classOf[CoreAnnotations.CharacterOffsetBeginAnnotation])
				val endOffset = annotatedToken.get(classOf[CoreAnnotations.CharacterOffsetEndAnnotation])
				OffsetToken(token, beginOffset, endOffset)
			}
	}

	def reverse(tokens: List[String]): String = tokens.mkString(" ")
}
