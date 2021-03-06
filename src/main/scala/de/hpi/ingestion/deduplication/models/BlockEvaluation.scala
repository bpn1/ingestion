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
  * Contains statistical information about a block generated by a blocking scheme.
  * @param jobid TimeUUID used as primary key identifying the job
  * @param schemetag Blocking Scheme tag used as primary key identifying the blocking scheme
  * @param data contains the statistical data about each block generates by this scheme
  * @param comment contains extra information about the data
  * @param pairscompleteness pairs completeness measure of this blocking scheme (% of duplicates that can be found with
  * this blocking scheme.
  * @param blockcount total number of blocks created
  * @param comparisoncount total number of comparisons done with this blocking scheme
  * @param xaxis label of the x-axis
  * @param yaxis label of the y-axis
  */
case class BlockEvaluation(
    jobid: UUID = UUIDs.timeBased(),
    schemetag: String,
    data: Set[BlockStats] = Set[BlockStats](),
    comment: Option[String] = None,
    pairscompleteness: Double,
    blockcount: Int,
    comparisoncount: BigInt,
    xaxis: Option[String] = Option("block keys"),
    yaxis: Option[String] = Option("# comparisons")
)
