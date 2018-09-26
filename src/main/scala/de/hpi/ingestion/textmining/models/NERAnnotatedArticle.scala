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

import de.hpi.fgis.utils.text.annotation.NamedEntity

/**
  * An article that was annotated via NER.
  * @param id the dataset specific id of the article
  * @param uuid a uuid generated for identification in the fuzzy matching NEL
  * @param title the title of the article
  * @param text the content of the article
  * @param nerentities a list of NER annotations
  */
case class NERAnnotatedArticle(
    id: String,
    uuid: UUID = UUID.randomUUID(),
    title: Option[String] = None,
    text: Option[String] = None,
    nerentities: List[NamedEntity] = Nil
) {
    def getText: String = text.getOrElse("")

    def entityNames: List[String] = {
        val text = getText
        nerentities
            .map { namedEntity => text.slice(namedEntity.start, namedEntity.end) }
            .distinct
    }
}
