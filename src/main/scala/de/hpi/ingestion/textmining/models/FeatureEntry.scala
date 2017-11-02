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

package de.hpi.ingestion.textmining.models

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.{LabeledPoint => LabeledPointDF}
import org.apache.spark.ml.linalg.{DenseVector => DenseVectorDF}

/**
  * Contains the data of a single entry for the classifier.
  *
  * @param article      article's name from which the feature entry was derived
  * @param offset       character offset where the alias occurred in the article
  * @param alias        alias found in a text
  * @param entity       possible entity the alias might be pointing to
  * @param link_score   score for the alias being a link
  * @param entity_score score for a link with this alias pointing to the entity
  * @param cosine_sim   cosine similarity of the context of the alias and the entity's article/context
  * @param correct      whether or not this alias actually points to the entity (only known for training)
  */
case class FeatureEntry(
    article: String,
    offset: Int,
    alias: String,
    entity: String,
    link_score: Double,
    entity_score: MultiFeature,
    cosine_sim: MultiFeature,
    correct: Boolean = false
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

    def labeledPointDF(): LabeledPointDF = {
        val point = this.labeledPoint()
        val vector = new DenseVectorDF(point.features.toArray)
        LabeledPointDF(point.label, vector)
    }
}
