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
