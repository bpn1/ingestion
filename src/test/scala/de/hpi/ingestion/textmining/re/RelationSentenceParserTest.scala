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

package de.hpi.ingestion.textmining.re

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.TestData
import de.hpi.ingestion.textmining.models.Sentence
import de.hpi.ingestion.textmining.tokenizer.{CleanWhitespaceTokenizer, IngestionTokenizer, SentenceTokenizer}
import org.scalatest.{FlatSpec, Matchers}

class RelationSentenceParserTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Wikipedia text" should "be split into exactly these Sentences with these entities" in {
		val parsedEntry = TestData.bigLinkExtenderParsedEntry()
		val sentenceTokenizer = IngestionTokenizer(new SentenceTokenizer, false, false)
		val tokenizer = IngestionTokenizer(new CleanWhitespaceTokenizer, false, false)
		val sentences = RelationSentenceParser.entryToSentencesWithEntities(parsedEntry, sentenceTokenizer, tokenizer)
		sentences shouldEqual TestData.sentenceList()
	}

	"Sentences" should "not contain these countries and cities as entities" in {
		val sentences = sc.parallelize(TestData.alternativeSentenceList())
		val relations = sc.parallelize(TestData.relationList())
		val companies = TestData.companySet()
		val filteredSentences = RelationSentenceParser.filterSentences(
			sentences,
			relations,
			companies,
			sc).collect.toSet
		filteredSentences shouldEqual TestData.alternativeSentenceListFiltered().toSet
	}

	"Wikipedia articles" should "be split into exactly these Sentences with these entities" in {
		val entries = sc.parallelize(
			(Set(TestData.bigLinkExtenderParsedEntry()) ++ TestData.linkExtenderExtendedParsedEntry()).toList
		)
		val relations = sc.parallelize(TestData.relationList())
		val wikidata = sc.parallelize(TestData.wikiDataCompanies())
		val input = List(entries).toAnyRDD() ++ List(relations).toAnyRDD() ++ List(wikidata).toAnyRDD()
		val sentences = RelationSentenceParser.run(input, sc)
			.fromAnyRDD[Sentence]()
			.head
			.collect
			.toList
		val expectedSentences =  TestData.alternativeSentenceListFiltered()
		sentences shouldEqual expectedSentences
	}
}
