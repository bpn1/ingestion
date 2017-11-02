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

import de.hpi.ingestion.datalake.models.DLImportEntity
import scala.reflect.runtime.universe._

/**
  * Represents a Wikidata entity.
  * @param id the Wikidata Id of the entity.
  * @param aliases list of aliases
  * @param description the Wikidata description
  * @param entitytype the type given by wikidata, which is either property or item.
  * @param wikiname the german Wikipedia name of the entity
  * @param enwikiname the english Wikipedia name of the entity
  * @param instancetype the superclass of this objects class set in the TagEntities job
  * @param label the label of the entity
  * @param data Map containing the Wikidata claims with their target and value
  */
case class WikidataEntity(
    id: String,
    var aliases: List[String] = Nil,
    var description: Option[String] = None,
    var entitytype: Option[String] = None,
    var wikiname: Option[String] = None,
    var enwikiname: Option[String] = None,
    var instancetype: Option[String] = None,
    var label: Option[String] = None,
    var data: Map[String, List[String]] = Map[String, List[String]]()
) extends DLImportEntity {
    def get(attribute: String): List[String] = {
        if(this.fieldNames[WikidataEntity].contains(attribute)) {
            val field = this.getClass.getDeclaredField(attribute)
            field.setAccessible(true)
            attribute match {
                case "id" => List(this.id)
                case "aliases" => this.aliases
                case "description" | "entititype" | "wikiname" | "enwikiname" | "instancetype" | "label" => {
                    val value = field.get(this).asInstanceOf[Option[String]]
                    value.toList
                }
                case _ => Nil
            }
        } else {
            data.getOrElse(attribute, Nil)
        }
    }
}
