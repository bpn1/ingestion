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

package de.hpi.ingestion.datalake

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.mock.{MockImport, MockSubjectImport}
import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}

class DataLakeImportImplementationTest extends FlatSpec with Matchers with SharedSparkContext {
	"normalizeProperties" should "normalize the properties of an entity" in {
		val job = new MockImport
		val entity = TestData.testEntity
		val mapping = TestData.normalizationMapping
		val strategies = TestData.strategyMapping
		val properties = job.normalizeProperties(entity, mapping, strategies)
		val expected = TestData.propertyMapping
		properties shouldEqual expected
	}

	"Entities" should "be translated" in {
		val job = new MockSubjectImport
		job.inputEntities = sc.parallelize(TestData.translationEntities)
		job.run(sc)
		val subjects = job.subjects.collect.toList
		val expectedSubjects = TestData.translatedSubjects
		subjects shouldEqual expectedSubjects
	}

	"filterEntities" should "filter no element by default" in {
		val job = new MockImport
		val entities = TestData.testEntities
		val filteredEntities = entities.filter(job.filterEntities)
		filteredEntities shouldEqual entities
	}

	"extractLegalForm" should "extract the legal form from a given name" in {
		val job = new MockImport
		val classifier = job.classifier
		val companyNames = TestData.companyNames
		companyNames.foreach { case (name, expected) =>
			val legalForms = job.extractLegalForm(name, classifier)
			legalForms shouldEqual expected
		}
		job.extractLegalForm(null, classifier) shouldEqual None
	}

	"run" should "import a new datasource to the datalake" in {
		val job = new MockImport
		job.inputEntities = sc.parallelize(TestData.testEntities)
		job.run(sc)
		val output = job.subjects.collect.toList
		val expected = TestData.output
		(output, expected).zipped.foreach { case (subject, expectedSubject) =>
			subject.name shouldEqual expectedSubject.name
		}
	}
}
