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
import de.hpi.ingestion.implicits.CollectionImplicits._

class KompassDataLakeImportTest extends FlatSpec with Matchers with SharedSparkContext {
	"extractAddress" should "extract and normalize street, postal, city and country from the address" in {
		val addresses = TestData.unnormalizedAddresses
		val normalizedAddresses = addresses.map(KompassDataLakeImport.extractAddress)
		val expected = TestData.normalizedAddresses
		normalizedAddresses shouldEqual expected
	}
}
