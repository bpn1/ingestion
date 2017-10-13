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

package de.hpi.ingestion.dataimport.spiegel

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.textmining.models.TrieAliasArticle
import de.hpi.ingestion.textmining.preprocessing.AliasTrieSearch
import de.hpi.ingestion.textmining.{TestData => TextTestData}

class SpiegelImportTest extends FlatSpec with Matchers with SharedSparkContext {
	"Spiegel articles" should "be parsed" in {
		val job = new SpiegelImport
		job.spiegelDump = sc.parallelize(TestData.spiegelFile())
		job.run(sc)
		val articles = job.parsedArticles.collect.toSet
		val expectedArticles = TestData.parsedArticles().toSet
		articles shouldEqual expectedArticles
	}

	"Article values" should "be extracted" in {
		val job = new SpiegelImport
		val articles = TestData.spiegelJson().map(job.fillEntityValues)
		val expectedArticles = TestData.parsedArticles()
		articles shouldEqual expectedArticles
	}

	"Article text" should "be extracted" in {
		val extractedContents = TestData.spiegelPages().map(SpiegelImport.extractArticleText)
		val expectedContents = TestData.pageTexts()
		extractedContents shouldEqual expectedContents
	}
}
