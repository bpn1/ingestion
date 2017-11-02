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

package de.hpi.ingestion.dataimport.kompass.models

import java.util.UUID

import de.hpi.ingestion.datalake.models.DLImportEntity

/**
  * Class representing an entity of the KompassDump.
  * @param name title of page in Kompass data
  * @param data all other information about the entity
  */
case class KompassEntity(
    var name: Option[String] = None,
    var instancetype: Option[String] = None,
    var data: Map[String, List[String]] = Map[String, List[String]](),
    id: UUID = UUID.randomUUID()
) extends DLImportEntity {
    def get(attribute: String): List[String] = {
        if(this.fieldNames[KompassEntity]().contains(attribute)) {
            val field = this.getClass.getDeclaredField(attribute)
            field.setAccessible(true)
            attribute match {
                case "id" | "name" => {
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
