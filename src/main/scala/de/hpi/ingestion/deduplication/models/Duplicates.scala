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

/**
  * Possible duplicates of subject
  * @param subject_id UUID of the subject
  * @param subject_name name
  * @param datasource source where the candidates come from
  * @param candidates List of duplicate candidates
  */
case class Duplicates(
    subject_id: UUID,
    subject_name: Option[String] = None,
    datasource: String,
    candidates: List[Candidate] = Nil
)
