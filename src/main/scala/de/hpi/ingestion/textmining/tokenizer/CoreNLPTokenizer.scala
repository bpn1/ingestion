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
		val tokenizer = new TokenizerAnnotator(false, "German")
		tokenizer.annotate(annotation)
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
