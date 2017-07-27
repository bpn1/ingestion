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
import de.hpi.ingestion.datalake.models.Version
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class FindRelationsTest extends FlatSpec with SharedSparkContext with Matchers {
	"Subject relations" should "be found" in {
		val nameMap = TestData.resolvedNameMap()
		val subjects = TestData.unresolvedSubjects()
			.map(FindRelations.findRelations(_, nameMap, Version("FindRelationsTest", Nil, sc, false, None)))
			.map(subject => (subject.id, subject.name, subject.properties, subject.relations))
		val expectedSubjects = TestData.resolvedSubjects()
			.map(subject => (subject.id, subject.name, subject.properties, subject.relations))
		subjects shouldEqual expectedSubjects
	}

	"Name resolve map" should "contain all resolvable names" in {
		val subjects = sc.parallelize(TestData.unresolvedSubjects())
		val resolvedNames = FindRelations.resolvableNamesMap(subjects)
		val expectedMap = TestData.resolvedNameMap()
		resolvedNames shouldEqual expectedMap
	}

}
