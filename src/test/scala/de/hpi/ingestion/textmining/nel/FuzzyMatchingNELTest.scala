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
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.deduplication.{TestData => DeduplicationTestData}

class FuzzyMatchingNELTest extends FlatSpec with Matchers with SharedSparkContext {
    "Entities" should "be extracted from the articles" in {
        val articles = TestData.fuzzyMatchingArticles
        val entities = articles.flatMap(_.entityNames)
        val expectedEntities = TestData.fuzzyMatchingEntities
        entities shouldEqual expectedEntities
    }

    they should "be transformed into Subjects" in {
        val job = new FuzzyMatchingNEL
        val articles = TestData.fuzzyMatchingArticles
        job.settings = job.settings ++ Map("stagingTable" -> "test")
        job.annotatedArticles = sc.parallelize(articles)
        val subjects = job.entitySubjects.collect.toList.sortBy(subject => (subject.id, subject.name))
        val expectedSubjects = TestData.fuzzyMatchingEntitySubjects.sortBy(subject => (subject.id, subject.name))
        subjects.zip(expectedSubjects).foreach { case (subject, expectedSubject) =>
            subject.id shouldEqual expectedSubject.id
            subject.master shouldEqual expectedSubject.master
            subject.datasource shouldEqual expectedSubject.datasource
            subject.name shouldEqual expectedSubject.name
        }
    }

    they should "be matched to Subjects" in {
        val job = new FuzzyMatchingNEL
        job.scoreConfigSettings = DeduplicationTestData.simpleTestConfig
        job.settings = job.settings ++ Map("stagingTable" -> "test", "confidence" -> "0.92")
        job.annotatedArticles = sc.parallelize(TestData.fuzzyMatchingArticles)
        job.subjects = sc.parallelize(TestData.fuzzyMatchingSubjects)
        job.run(sc)
        val matches = job.matches
            .collect
            .toList
            .toSet
        val expectedMatches = TestData.fuzzyMatchingMatches
        matches shouldEqual expectedMatches
    }
}
