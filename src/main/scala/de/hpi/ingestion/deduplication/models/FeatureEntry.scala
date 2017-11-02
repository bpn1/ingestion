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
