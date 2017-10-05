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

package de.hpi.ingestion.textmining.preprocessing

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.TestData
import org.scalatest.{FlatSpec, Matchers}

class FeatureCsvExportTest extends FlatSpec with SharedSparkContext with Matchers {
	"Features csv string" should "be exactly this string" in {
		val input = List(sc.parallelize(TestData.featureEntriesList())).toAnyRDD()
		val featuresCsvString = FeatureCsvExport.run(input, sc)
			.fromAnyRDD[String]()
			.head
			.collect
			.head
		featuresCsvString shouldEqual TestData.featuresCsvString()
	}
}
