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

package de.hpi.ingestion.dataimport.wikidata.models

/**
  * Represents the subclass data of a Wikidata entity.
  * @param id Wikidata Id of the entity
  * @param label label of the entity
  * @param data filtered data map containing only the subclass of and instance of properties
  * @param classList list of subclasses of this entry
  */
case class SubclassEntry(
    id: String,
    var label: String = "",
    var data: Map[String, List[String]] = Map[String, List[String]](),
    var classList: List[String] = Nil)
