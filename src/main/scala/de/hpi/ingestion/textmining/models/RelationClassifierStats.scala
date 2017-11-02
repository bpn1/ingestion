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

import java.util.UUID
import com.datastax.driver.core.utils.UUIDs
import de.hpi.ingestion.deduplication.models.PrecisionRecallDataTuple

/**
  * Contains statistics about a relation classifier.
  * @param id unique id used as primary key
  * @param rel relation that was classified
  * @param sentenceswithrelation number of sentences that contain the classified relation
  * @param sentenceswithnorelation number of sentences that don't contain the classified relation
  * @param average contains the average precision, recall and f-score
  * @param data contains all precision, recall and f-score data (multiple when cross-validating)
  * @param comment described this data entry
  */
case class RelationClassifierStats(
    id: UUID = UUIDs.timeBased(),
    rel: String,
    sentenceswithrelation: Int,
    sentenceswithnorelation: Int,
    average: PrecisionRecallDataTuple,
    data: List[PrecisionRecallDataTuple] = Nil,
    comment: Option[String] = None
)
