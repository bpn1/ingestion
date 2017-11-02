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
import com.datastax.driver.core.utils.UUIDs

/**
  * Case class for corresponding sim_measure_stats cassandra table
  * @param id unique UUID used as primary key
  * @param data Array containing (threshold, precision, recall, f1Score) tuple
  * @param comment String containing a comment or description of the data
  * @param xaxis String containing the label of the x-axis
  * @param yaxis String containing the label of the y-axis
  */
case class SimilarityMeasureStats(
    id: UUID = UUIDs.timeBased(),
    data: List[PrecisionRecallDataTuple] = Nil,
    comment: Option[String] = None,
    xaxis: Option[String] = None,
    yaxis: Option[String] = None
)
