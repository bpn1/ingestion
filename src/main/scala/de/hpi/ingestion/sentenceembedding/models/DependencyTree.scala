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
  * Represents a sentence and its dependency parse tree.
  * @param id id of the sentence
  * @param sentence the sentence itself
  * @param dependency_tree the sentence's dependency parse tree
  * @param relations the extracted relations as triples of token lists
  * @param pattern the pattern of the dependency parse tree
  * @param patternfrequency the number of times the pattern of the dependency parse tree occurred
  */
case class DependencyTree(
    id: Long,
    sentence: Option[String] = None,
    dependency_tree: Option[String] = None,
    relations: List[(List[String], List[String], List[String])] = Nil,
    pattern: Option[String] = None,
    patternfrequency: Option[Int] = None
)
