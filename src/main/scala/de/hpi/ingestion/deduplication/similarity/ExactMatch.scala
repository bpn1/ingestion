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

package de.hpi.ingestion.deduplication.similarity

/**
  * An abstract binary similarity measure for exact matching
  */
object ExactMatch extends SimilarityMeasure[Any] {
    /**
      * Comparing the given objects on exact matching
      * @param x object to be compared to y
      * @param y object to be compared to x
      * @param u has no specific use in here
      * @return 1.0 if given objects match exactly, 0.0 otherwise
      */
    override def compare(x: Any, y: Any, u: Int = 1) = if(x == y) 1.0 else 0.0
}
