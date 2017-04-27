package de.hpi.ingestion.textmining

import java.util.StringTokenizer
import edu.stanford.nlp.simple.GermanDocument
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.annotation.tailrec

/**
  * Trait used for textmining tokenizers. Declares methods to tokenize a text and create a text from a list of tokens.
  */
trait Tokenizer extends Serializable {
	def tokenize(x: String): List[String]

	def reverse(x: List[String]): String
}

/**
  * Tokenizes the given text on spaces.
  */
class WhitespaceTokenizer() extends Tokenizer {
	def tokenize(txt: String) = txt.split(" ").toList

	def reverse(tokens: List[String]) = tokens.mkString(" ")
}

/**
  * Uses the WhitespaceTokenizer but removes control characters and removes whitespace.
  */
class CleanWhitespaceTokenizer() extends Tokenizer {
	def stripAll(s: String, bad: String): String = {
		// Source: http://stackoverflow.com/a/17995434
		@tailrec def start(n: Int): String = {
			if (n == s.length) {
				""
			} else if (bad.indexOf(s.charAt(n)) < 0) {
				end(n, s.length)
			} else {
				start(1 + n)
			}
		}

		@tailrec def end(a: Int, n: Int): String = {
			if (n <= a) {
				s.substring(a, n)
			}
			else if (bad.indexOf(s.charAt(n - 1)) < 0) {
				s.substring(a, n)
			}
			else {
				end(a, n - 1)
			}
		}

		start(0)
	}

	def tokenize(txt: String) = {
		val delimiters = " \n"
		val badCharacters = "().!?,;:'`\"„“"
		val stringTokenizer = new StringTokenizer(txt, delimiters)
		val tokens = new ListBuffer[String]()

		while (stringTokenizer.hasMoreTokens) {
			tokens += stringTokenizer.nextToken()
		}

		tokens
			.map(token => stripAll(token, badCharacters))
			.toList
	}

	def reverse(tokens: List[String]) = tokens.mkString(" ")
}

/**
  * Uses the CoreNLPTokenizer but removes control characters from the resulting tokens.
  */
class CleanCoreNLPTokenizer() extends CoreNLPTokenizer {
	override def tokenize(txt: String) = {
		val tokens = super.tokenize(txt)
		val badTokens = Set[String](".", "!", "?", ",", ";", ":")
		tokens.filter(token => !badTokens.contains(token))
	}
}

/**
  * Uses the CoreNLP Simple German API to tokenize the given text.
  */
class CoreNLPTokenizer() extends Tokenizer {
	def tokenize(txt: String) = {
		new GermanDocument(txt)
			.sentences
			.asScala
			.toList
			.flatMap(_.words.asScala.toList)
	}

	def reverse(tokens: List[String]) = tokens.mkString(" ")
}
