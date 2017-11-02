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
