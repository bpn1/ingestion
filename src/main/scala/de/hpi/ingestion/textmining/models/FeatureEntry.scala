package de.hpi.ingestion.textmining.models

import java.util.UUID
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Contains the data of a single entry for the classifier.
  *
  * @param alias       alias found in a text
  * @param entity      possible entity the alias might be pointing to
  * @param prob_link   probability that the alias is a link
  * @param prob_entity probability that the alias points to the entity given it is a link
  *                    with additional second order features
  * @param cosine_sim  cosine similarity of the context of the alias and the entities article/context
  * @param correct     whether or not this alias actually points to the entity
  * @param id          unique uuid used as Cassandra primary key
  */
case class FeatureEntry(
	alias: String,
	entity: String,
	prob_link: Double,
	prob_entity: MultiFeature,
	cosine_sim: MultiFeature,
	correct: Boolean = false,
	id: UUID = UUID.randomUUID()
) {
	/**
	  * Returns a Labeled Point containing the data of this entries features and if the entry is correct.
	  *
	  * @return Labeled Point containing the features and the label
	  */
	def labeledPoint(): LabeledPoint = {
		val features = new DenseVector(Array(
			prob_link,
			prob_entity.value, prob_entity.rank, prob_entity.deltaTop, prob_entity.deltaSuccessor,
			cosine_sim.value, cosine_sim.rank, cosine_sim.deltaTop, cosine_sim.deltaSuccessor))
		val label = if(correct) 1.0 else 0.0
		LabeledPoint(label, features)
	}
}
