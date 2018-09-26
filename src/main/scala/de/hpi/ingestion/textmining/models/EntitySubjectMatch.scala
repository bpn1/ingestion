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

/**
  * A link of an NER entity to a Subject in the knowledge base.
  * @param subject_id id of the matched subject
  * @param subject_name name of the matched subject
  * @param article_id uuid of the article in which the entity appears
  * @param entity_name the entity itself
  * @param article_source the name of the dataset from which the article is
  * @param score the score created by the used similarity measures
  */
case class EntitySubjectMatch(
    subject_id: UUID,
    subject_name: Option[String],
    article_id: UUID,
    entity_name: String,
    article_source: String,
    score: Double
)
