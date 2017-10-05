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
		val testTrieStreamFunction = TextTestData.fullTrieStream("/spiegel/triealiases") _
		AliasTrieSearch.trieStreamFunction = testTrieStreamFunction

		val job1 = new ArticleTrieSearch
		job1.nelArticles = sc.parallelize(TestData.aliasSearchArticles())
		job1.run(sc)
		val foundAliases1 = job1.foundAliases.collect.toSet
		foundAliases1 shouldEqual TestData.foundTrieAliases()

		val job2 = new ArticleTrieSearch
		job2.nelArticles = sc.parallelize(TestData.realAliasSearchArticles())
		job2.run(sc)
		val foundAliases2 = job2.foundAliases.collect.toSet
		foundAliases2 shouldEqual TestData.realFoundTrieAliases()

		AliasTrieSearch.trieStreamFunction = oldTrieStreamFunction
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
