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

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class MasterUpdateTest extends FlatSpec with Matchers with SharedSparkContext {
	"Master nodes" should "be updated" in {
		val job = new MasterUpdate
		job.subjects = sc.parallelize(TestData.outdatedMasters())
		job.run(sc)
		val updatedMasters = job.updatedMasters.collect().toList.sortBy(_.id)
		val expectedMasters = TestData.updatedMasters().sortBy(_.id)
		updatedMasters should have length expectedMasters.length
		updatedMasters.zip(expectedMasters).foreach { case (master, expectedMaster) =>
			master.id shouldEqual expectedMaster.id
			master.name shouldEqual expectedMaster.name
			master.aliases shouldEqual expectedMaster.aliases
			master.category shouldEqual expectedMaster.category
			master.properties shouldEqual expectedMaster.properties
			master.relations shouldEqual expectedMaster.relations
		}
	}
}
