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

package de.hpi.ingestion.sentenceembedding.models

/**
  * Represents a pattern of a dependency parse tree and its aggregated data.
  * @param pattern the pattern of the dependency parse tree
  * @param frequency the number of times the pattern occurred
  * @param sentences the sentences with this dependency parse tree pattern
  */
case class DependencyTreePattern(
    pattern: String,
    frequency: Option[Int] = None,
    sentences: List[String] = Nil
)
