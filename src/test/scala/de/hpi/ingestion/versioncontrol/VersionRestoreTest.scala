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

package de.hpi.ingestion.versioncontrol

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.scalatest.{FlatSpec, Matchers}

class VersionRestoreTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Subject data" should "be restored" in {
		val subjects = sc.parallelize(TestData.diffSubjects() ++ TestData.additionalRestorationSubjects())
		val versions = Array(TestData.versionsToCompare()._1.toString)
		val rddList = List(subjects).toAnyRDD()
		val restoredSubjects = VersionRestore.run(rddList, sc, versions)
			.fromAnyRDD[Subject]()
			.head
			.collect
			.sortBy(_.id)

		val zippedSubjects = restoredSubjects.zip(TestData.restoredSubjects().sortBy(_.id))
		restoredSubjects.length shouldEqual zippedSubjects.length
		zippedSubjects.foreach { case (restoredSubject, testSubject) =>
			restoredSubject.name shouldEqual testSubject.name
			restoredSubject.master shouldEqual testSubject.master
			restoredSubject.aliases shouldEqual testSubject.aliases
			restoredSubject.category shouldEqual testSubject.category
			restoredSubject.properties shouldEqual testSubject.properties
			restoredSubject.relations shouldEqual testSubject.relations
		}
	}

	"Version Restore assertion" should "return false if there is not exactly one versions provided" in {
		val successArgs = Array("v1")
		val failArgsEmpty = Array[String]()
		val failArgsTooMany = Array("v1", "v2")
		VersionRestore.assertConditions(successArgs) shouldBe true
		VersionRestore.assertConditions(failArgsEmpty) shouldBe false
		VersionRestore.assertConditions(failArgsTooMany) shouldBe false
	}
}
