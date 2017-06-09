package de.hpi.ingestion.deduplication.models

import java.util.UUID

import de.hpi.ingestion.datalake.models.Subject
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector

/**
  * Contains the data of a single entry for the classifier
  * @param id id
  * @param subject subject
  * @param staging staging
  * @param scores scores of the similarity measures
  * @param correct whether the subjects are duplicates or not
  */
case class FeatureEntry(
	id: UUID = UUID.randomUUID(),
	subject: Subject,
	staging: Subject,
	scores: Map[String, List[Double]] = Map(),
	correct: Boolean = false
) {
	/**
	  * Returns a labeled Point
	  * @return Labeled Point containing a label and the features
	  */
	def labeledPoint: LabeledPoint = {
		val features = new DenseVector(scores.values.toArray.flatten)
		val label = if(correct) 1.0 else 0.0
		LabeledPoint(label, features)
	}
}
