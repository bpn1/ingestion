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

package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._

class MasterConnectingTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {
	"Master nodes" should "be connected" in {
		val subjects = sc.parallelize(TestData.mergedSubjects)
		val input = List(subjects).toAnyRDD()
		val output = MasterConnecting
			.run(input, sc)
			.fromAnyRDD[Subject]()
			.head
			.collect
			.sortBy(_.id)
			.map(_.relations)

		val expected = TestData
			.connectedMasters
			.sortBy(_.id)
			.map(_.relations)

		output.length shouldEqual expected.length
		(output, expected).zipped.foreach { case (relations, expectedRelations) =>
			relations shouldEqual expectedRelations
		}
	}

	they should "be merged and connected" in {
		val subjects = sc.parallelize(TestData.inputSubjects())
		val input = List(subjects).toAnyRDD()
		val connectedMasters = MasterConnecting.run(input, sc)
			.fromAnyRDD[Subject]()
			.head
			.collect
			.toList
			.sortBy(_.id)
		val expectedMasters = TestData.mergedMasters().sortBy(_.id)
		connectedMasters.length shouldEqual expectedMasters.length
		connectedMasters.zip(expectedMasters).foreach { case (connectedMaster, expectedMaster) =>
			connectedMaster.relations shouldEqual expectedMaster.relations
		}
	}
}
