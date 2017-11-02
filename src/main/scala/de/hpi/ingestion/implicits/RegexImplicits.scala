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

package de.hpi.ingestion.implicits

/**
  * Contains implicit classes extending Scala Regex.
  */
object RegexImplicits {
    /**
      * Implicit class for using regex in pattern matching
      * @param sc String Context
      */
    implicit class Regex(sc: StringContext) {
        /**
          * This function helps using string interpolation for regexes as used in the normalize strategies.
          * Source: http://stackoverflow.com/a/16256935/6625021
          * @return matcher
          */
        def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
    }
}
