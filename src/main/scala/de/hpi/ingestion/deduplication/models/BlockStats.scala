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

/**
  * Contains the statistical data of a single block.
  * @param key key used to generate this block
  * @param numsubjects number of Subjects in this block
  * @param numstaging number of staged Subjects in this block
  */
case class BlockStats(
    key: String,
    numsubjects: Int,
    numstaging: Int,
    precision: Double = 0.0
) {
    /**
      * Calculates the number of comparisons resulting from this Block.
      * @return number of comparisons done in this Block
      */
    def numComparisons: Long = numsubjects * numstaging
}
