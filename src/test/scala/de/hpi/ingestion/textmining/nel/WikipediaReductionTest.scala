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

import de.hpi.ingestion.textmining.TestData.parsedWikipediaWithTextsSet
import de.hpi.ingestion.textmining.models.TrieAliasArticle
import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class WikipediaReductionTest extends FlatSpec with SharedSparkContext with Matchers {
	"Reduced Wikipedia articles" should "not be empty" in {
		val job = new WikipediaReduction
		job.parsedWikipedia = sc.parallelize(parsedWikipediaWithTextsSet().toList)
		job.run(sc)
		job.reducedArticles should not be empty
	}

	they should "be exactly these articles" in {
		val job = new WikipediaReduction
		job.parsedWikipedia = sc.parallelize(parsedWikipediaWithTextsSet().toList)
		job.run(sc)
		val reducedArticles = job.reducedArticles
			.collect
			.toSet
		reducedArticles shouldEqual TestData.reducedWikipediaArticles()
	}
}
