package de.hpi.ingestion.textmining.tokenizer

import java.util.StringTokenizer

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

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
				println("good stuff")
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
