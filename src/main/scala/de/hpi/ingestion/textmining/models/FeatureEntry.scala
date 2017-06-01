package de.hpi.ingestion.textmining.models

import java.util.UUID
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Contains the data of a single entry for the classifier.
  *
  * @param alias        alias found in a text
  * @param entity       possible entity the alias might be pointing to
  * @param link_score   score for the alias being a link
  * @param entity_score score for a link with this alias pointing to the entity
  * @param cosine_sim   cosine similarity of the context of the alias and the entity's article/context
  * @param correct      whether or not this alias actually points to the entity (only known for training)
  * @param id           unique uuid used as Cassandra primary key
  */
case class FeatureEntry(
	alias: String,
	entity: String,
	link_score: Double,
	entity_score: MultiFeature,
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
			link_score,
			entity_score.value, entity_score.rank, entity_score.delta_top, entity_score.delta_successor,
			cosine_sim.value, cosine_sim.rank, cosine_sim.delta_top, cosine_sim.delta_successor))
		val label = if(correct) 1.0 else 0.0
		LabeledPoint(label, features)
	}
}
