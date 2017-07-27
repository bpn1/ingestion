/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.tokenizer

import java.util.StringTokenizer
import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
  * Uses the WhitespaceTokenizer but removes control characters and removes whitespace.
  */
class CleanWhitespaceTokenizer() extends Tokenizer {

	/**
	  * Removes all leading and trailing characters that are not wanted.
	  * Source: http://stackoverflow.com/a/17995434
	  * @param s String to clean
	  * @param bad String of characters to filter
	  * @return cleaned String not containg any of the bad characters as first or last characters
	  */
	def stripAll(s: String, bad: String): String = {
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
			if(bad.indexOf(s.charAt(n - 1)) < 0) {
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
