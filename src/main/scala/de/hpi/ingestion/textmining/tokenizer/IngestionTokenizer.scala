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

import de.hpi.ingestion.textmining.models.OffsetToken
import scala.io.Source
import scala.util.Try

/**
  * An advanced tokenizer that uses a given tokenizer, removes stop words if flag is set and stems the tokens
  *
  * @param tokenizer       Tokenizer to use
  * @param removeStopwords flag for removing stop words
  * @param stem            flag for stemming
  */
case class IngestionTokenizer(
	tokenizer: Tokenizer,
	removeStopwords: Boolean = false,
	stem: Boolean = false
) extends Serializable {
	val stopwordsPath = "german_stopwords.txt"
	val stopwords: Set[String] = Source.fromURL(getClass.getResource(s"/$stopwordsPath"), "UTF-8")
		.getLines()
		.filter(_.head != ';')
		.toSet

	/**
	  * Tokenizes a text, removes stop words and stems the tokens.
	  *
	  * @param text text to be processed into tokens
	  * @return a List of all remaining stemmed words
	  */
	def process(text: String): List[String] = {
		val stemmer = new AccessibleGermanStemmer
		var tokens = tokenizer.tokenize(text)
		if(removeStopwords) tokens = tokens.filterNot(stopwords)
		if(stem) tokens = tokens.map(stemmer.stem)
		tokens
	}

	/**
	  * Tokenizes a text with offset annotations, removes stop words and stems the tokens.
	  *
	  * @param text text to be processed into tokens
	  * @return a List of all remaining stemmed words with their offsets
	  */
	def processWithOffsets(text: String): List[OffsetToken] = {
		val stemmer = new AccessibleGermanStemmer
		var tokens = tokenizer.tokenizeWithOffsets(text)
		if(removeStopwords) tokens = tokens.filter(offsetToken => !stopwords.contains(offsetToken.token))
		if(stem) tokens = tokens.map { offsetToken =>
			OffsetToken(stemmer.stem(offsetToken.token), offsetToken.beginOffset, offsetToken.endOffset)
		}
		tokens
	}

	/**
	  * Removes stopwords and stems a list of tokens.
	  *
	  * @param tokens List of tokens to be processed
	  * @return List of stemmed words which were not stopwords
	  */
	def process(tokens: List[String]): List[String] = {
		val stemmer = new AccessibleGermanStemmer
		tokens
			.filterNot(token => removeStopwords && stopwords.contains(token))
			.map {
				case token if stem => stemmer.stem(token)
				case token if !stem => token
			}
	}

	/**
	  * Tokenizes a text.
	  *
	  * @param text text to be tokenized
	  * @return List of tokens
	  */
	def onlyTokenize(text: String): List[String] = {
		tokenizer.tokenize(text)
	}

	/**
	  * Tokenizes a text with offsets.
	  *
	  * @param text text to be tokenized
	  * @return List of tokens with offsets
	  */
	def onlyTokenizeWithOffset(text: String): List[OffsetToken] = {
		tokenizer.tokenizeWithOffsets(text)
	}

	/**
	  * Reverts the tokenization step. Only useful when the tokens were not stemmed and the stopwords were not filtered.
	  *
	  * @param tokens List of tokens to join
	  * @return String of the tokens joined
	  */
	def reverse(tokens: List[String]): String = tokenizer.reverse(tokens)
}

/**
  * Companion Object for IngestionTokenizer class implementing apply methods for a default tokenizer.
  */
object IngestionTokenizer {
	/**
	  * Creates default tokenizer which uses the CoreNLPTokenizer, removes stopwords and stems.
	  *
	  * @return returns tokenizer which uses the CoreNLPTokenizer, removes stopwords and stems
	  */
	def apply(): IngestionTokenizer = new IngestionTokenizer(new CoreNLPTokenizer, true, true)

	/**
	  * Creates default tokenizer which uses the CoreNLPTokenizer and takes parameters for removing stopwords and
	  * stemming.
	  *
	  * @param stopWords flag for removing stopwords
	  * @param stem      flag for stemming
	  * @return returns tokenizer which uses the CoreNLPTokenizer and removes stopwords and stems according to the flags
	  */
	def apply(stopWords: Boolean, stem: Boolean): IngestionTokenizer = {
		new IngestionTokenizer(new CoreNLPTokenizer, stopWords, stem)
	}

	/**
	  * Parses an Array of String arguments to create a Tokenizer. If the arguments are not well-formed the default
	  * values of the default Tokenizer will be used.
	  *
	  * @param args Array of arguments
	  * @return Tokenizer corresponding to the arguments if they are well-formed
	  */
	def apply(args: List[String]): IngestionTokenizer = {
		val tokenizer = if(args.length < 1) {
			new CoreNLPTokenizer
		} else {
			args.head match {
				case "WhitespaceTokenizer" => new WhitespaceTokenizer
				case "CleanWhitespaceTokenizer" => new CleanWhitespaceTokenizer
				case "CleanCoreNLPTokenizer" => new CleanCoreNLPTokenizer
				case "SentenceTokenizer" => new SentenceTokenizer
				case "CoreNLPTokenizer" | _ => new CoreNLPTokenizer
			}
		}
		val filter = args.length < 2 || Try(args(1).toBoolean).getOrElse(false)
		val stem = args.length < 3 || Try(args(2).toBoolean).getOrElse(false)
		new IngestionTokenizer(tokenizer, filter, stem)
	}
}
