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

package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class TagEntitiesTest extends FlatSpec with SharedSparkContext with Matchers {
    "Shorter paths" should "be added" in {
        val oldClasses = TestData.oldClassMap()
        val newClasses = TestData.newClassMap()
        val classMap = TagEntities.addShorterPaths(newClasses, oldClasses)
        val expectedMap = TestData.classMap()
        classMap shouldEqual expectedMap
    }

    "Subclass map" should "be built" in {
        val entries = sc.parallelize(TestData.subclassOfProperties())
        val classMap = TagEntities.buildSubclassMap(entries, TestData.classesToTag())
        val expectedMap = TestData.classMap()
        classMap shouldEqual expectedMap
    }

    it should "be properly reduced" in {
        val reducedSubclasses = TestData.reducableClassMaps().reduce(TagEntities.mergeSubclassMaps)
        val expectedSubclasses = TestData.classMap()
        reducedSubclasses shouldEqual expectedSubclasses
    }

    "Wikidata entity" should "be properly translated into SubclassEntry" in {
        val entries = TestData.classWikidataEntities()
            .map(TagEntities.translateToSubclassEntry)
        val expectedEntries = TestData.subclassEntries()
        entries shouldEqual expectedEntries
    }

    "Instance-of entities" should "be updated" in {
        val job = new TagEntities
        val pathKey = job.settings("wikidataPathProperty")
        val entries = sc.parallelize(TestData.subclassEntries())
        val updatedEntries = TagEntities.updateEntities(entries, TestData.classMap(), pathKey).collect.toSet
        val expectedEntries = TestData.updatedInstanceOfProperties().toSet
        updatedEntries shouldEqual expectedEntries
    }

    it should "be updated correctly" in {
        val job = new TagEntities
        val pathKey = job.settings("wikidataPathProperty")
        val entries = TestData.validInstanceOfProperties()
            .map(TagEntities.updateInstanceOfProperty(_, TestData.classMap(), pathKey))
        val expectedEntries = TestData.updatedInstanceOfProperties()
        entries shouldEqual expectedEntries
    }
}
