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

package de.hpi.ingestion.deduplication.models

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

object TestData {

	def featureEntries(): List[FeatureEntry] = {
		List(
			FeatureEntry(null, null, null, Map("a" -> List(1.0, 2.0)), true),
			FeatureEntry(null, null, null, Map("a" -> List(1.0), "b" -> List(2.0)), false),
			FeatureEntry(null, null, null, Map("a" -> List(0.2, 0.1)), false),
			FeatureEntry(null, null, null, Map("a" -> List(1.0, 2.0), "c" -> Nil), true),
			FeatureEntry(null, null, null, Map("asd" -> List(0.0, 0.1)), false))
	}

	def labeledPoints(): List[LabeledPoint] = {
		List(
			LabeledPoint(1.0, new DenseVector(Array(1.0, 2.0))),
			LabeledPoint(0.0, new DenseVector(Array(1.0, 2.0))),
			LabeledPoint(0.0, new DenseVector(Array(0.2, 0.1))),
			LabeledPoint(1.0, new DenseVector(Array(1.0, 2.0))),
			LabeledPoint(0.0, new DenseVector(Array(0.0, 0.1))))
	}
}
