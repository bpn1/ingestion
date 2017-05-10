package de.hpi.ingestion.textmining

import java.util.StringTokenizer

import de.hpi.ingestion.textmining.models.OffsetToken
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.TokenizerAnnotator
import edu.stanford.nlp.simple.GermanDocument
import edu.stanford.nlp.pipeline.Annotation

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.annotation.tailrec

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

/**
  * Tokenizes the given text on spaces.
  */
class WhitespaceTokenizer() extends Tokenizer {
	def tokenize(text: String): List[String] = text.split(" ").toList

	def reverse(tokens: List[String]): String = tokens.mkString(" ")
}

/**
  * Uses the WhitespaceTokenizer but removes control characters and removes whitespace.
  */
class CleanWhitespaceTokenizer() extends Tokenizer {
	def stripAll(s: String, bad: String): String = {
		// Source: http://stackoverflow.com/a/17995434
		@tailrec def start(n: Int): String = {
			if(n == s.length) {
				""
			} else if(bad.indexOf(s.charAt(n)) < 0) {
				end(n, s.length)
			} else {
				start(1 + n)
			}
		}

		@tailrec def end(a: Int, n: Int): String = {
			if(n <= a) {
				s.substring(a, n)
			}
			else if(bad.indexOf(s.charAt(n - 1)) < 0) {
				s.substring(a, n)
			}
			else {
				end(a, n - 1)
			}
		}

		start(0)
	}

	def tokenize(text: String): List[String] = {
		val delimiters = " \n"
		val badCharacters = "().!?,;:'`\"„“"
		val stringTokenizer = new StringTokenizer(text, delimiters)
		val tokens = new ListBuffer[String]()

		while(stringTokenizer.hasMoreTokens) {
			tokens += stringTokenizer.nextToken()
		}

		tokens
			.map(token => stripAll(token, badCharacters))
			.toList
	}

	def reverse(tokens: List[String]): String = tokens.mkString(" ")
}

/**
  * Uses the CoreNLPTokenizer but removes control characters from the resulting tokens.
  */
class CleanCoreNLPTokenizer() extends CoreNLPTokenizer {
	override def tokenize(text: String): List[String] = {
		val tokens = super.tokenize(text)
		val badTokens = Set[String](".", "!", "?", ",", ";", ":")
		tokens.filter(token => !badTokens.contains(token))
	}
}

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

/**
  * Uses the CoreNLP Simple German API to tokenize the given text into sentences.
  */
class CoreNLPSentenceTokenizer() extends CoreNLPTokenizer {
	override def tokenize(txt: String) = {
		new GermanDocument(txt)
			.sentences
			.asScala
			.toList
			.map(_.text())
	}
}
