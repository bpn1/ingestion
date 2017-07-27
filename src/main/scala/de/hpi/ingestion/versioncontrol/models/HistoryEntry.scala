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

package de.hpi.ingestion.versioncontrol.models

import java.util.UUID

/**
  * Contains the values of an older and a newer version for every field of a Subject.
  * @param id UUID of the Subject
  * @param name tuple of old and new name values
  * @param master tuple of old and new master values
  * @param aliases tuple of old and new alias values
  * @param category tuple of old and new category values
  * @param properties tuple of old and new values for every property
  * @param relations tuple of old and new values for every property of every relation
  */
case class HistoryEntry(
	id: UUID,
	name: Option[(List[String], List[String])] = None,
	master: Option[(List[String], List[String])] = None,
	aliases: Option[(List[String], List[String])] = None,
	category: Option[(List[String], List[String])] = None,
	properties: Map[String, Option[(List[String], List[String])]] = Map(),
	relations: Map[UUID, Map[String, Option[(List[String], List[String])]]] = Map()
)
