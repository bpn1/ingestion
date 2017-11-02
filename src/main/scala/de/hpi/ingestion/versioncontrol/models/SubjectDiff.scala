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

case class SubjectDiff(
    oldversion: UUID,
    newversion: UUID,
    id: UUID,
    var master: Option[String] = None,
    var datasource: Option[String] = None,
    var aliases: Option[String] = None,
    var category: Option[String] = None,
    var name: Option[String] = None,
    var properties: Option[String] = None,
    var relations: Option[String] = None
) {
    /**
      * Returns true if the current diff contains any changes
      * @return True if one of the diff fields in the SubjectDiff is not None
      */
    def hasChanges(): Boolean = {
        List(master, datasource, aliases, category, name, properties, relations).exists(_.isDefined)
    }
}
