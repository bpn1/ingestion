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

package de.hpi.ingestion.dataimport.kompass

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext

class KompassDataLakeImportTest extends FlatSpec with Matchers with SharedSparkContext {
    "Subject translation" should "translate all possible data" in {
        val job = new KompassDataLakeImport
        val entity = TestData.kompassEntity
        val version = TestData.version(sc)
        val mapping = job.normalizationSettings
        val strategies = Map.empty[String, List[String]]
        val classifier = job.classifier
        val subject = job.translateToSubject(entity, version, mapping, strategies, classifier)
        val expectedSubject = TestData.translatedSubject
        subject.name shouldEqual entity.name
        subject.category shouldEqual expectedSubject.category
        subject.properties shouldEqual expectedSubject.properties
    }
}
