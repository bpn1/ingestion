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

package de.hpi.ingestion.textmining.nel

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.TrieAlias
import de.hpi.ingestion.textmining.preprocessing.AliasTrieSearch
import de.hpi.ingestion.textmining.{TestData => TextTestData}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.scalatest.{FlatSpec, Matchers}

class ArticleTrieSearchTest extends FlatSpec with Matchers with SharedSparkContext {
	"Found aliases" should "be exactly these aliases" in {
		val oldTrieStreamFunction = AliasTrieSearch.trieStreamFunction
		val oldSettings = AliasTrieSearch.settings(false)
		AliasTrieSearch.parseConfig()
		val testTrieStreamFunction = TextTestData.fullTrieStream("/spiegel/triealiases") _
		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction

		val articles1 = TestData.aliasSearchArticles()
		val articles2 = TestData.realAliasSearchArticles()
		val input1 = List(sc.parallelize(articles1)).toAnyRDD()
		val input2 = List(sc.parallelize(articles2)).toAnyRDD()
		val foundAliases1 = ArticleTrieSearch.run(input1, sc)
			.fromAnyRDD[(String, List[TrieAlias])]()
			.head
			.collect
			.toSet
		val foundAliases2 = ArticleTrieSearch.run(input2, sc)
			.fromAnyRDD[(String, List[TrieAlias])]()
			.head
			.collect
			.toSet
		foundAliases1 shouldEqual TestData.foundTrieAliases()
		foundAliases2 shouldEqual TestData.realFoundTrieAliases()

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
		AliasTrieSearch.settings = oldSettings
	}

	"Enriched articles" should "be exactly these articles" in {
		val tokenizer = IngestionTokenizer(false, false)
		val trie = TextTestData.dataTrie(tokenizer, "/spiegel/triealiases")
		val enrichedArticles = TestData.aliasSearchArticles()
			.map(ArticleTrieSearch.findAliases(_, tokenizer, trie))
		val expectedArticles = TestData.foundAliasArticles()
		enrichedArticles shouldEqual expectedArticles
	}
}
