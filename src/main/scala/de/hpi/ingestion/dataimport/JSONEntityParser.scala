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

package de.hpi.ingestion.dataimport

import play.api.libs.json._

/**
  * Trait to parse JSON-Objects into Entities. Contains all methods needed to parse JSON into Scala Objects.
  * @tparam T type of the result Entities the JSON-Objects will be parsed to
  */
trait JSONEntityParser[T] extends JSONParser {

    /**
      * Parses a String containing a JSON-Object into an Entity of type T.
      * @param line String containing the JSON data
      * @return Entity containing the parsed data
      */
    def parseJSON(line: String): T = {
        val json = Json.parse(line)
        fillEntityValues(json)
    }

    /**
      * Extracts the JSON data from a JSON object to an Entity.
      * @param json JSON-Object containing the data
      * @return Entity containing the parsed data
      */
    def fillEntityValues(json: JsValue): T

    /**
      * Removes array syntax from JSON for parallel parsing of the JSON objects.
      * @param json JSON String to clean
      * @return cleaned JSON String in which each line is either a JSON object or empty
      */

    def cleanJSON(json: String): String = {
        json.replaceAll("^\\[|,$|, $|\\]$", "")
    }
}
