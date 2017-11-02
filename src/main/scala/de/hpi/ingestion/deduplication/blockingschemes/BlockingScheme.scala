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

package de.hpi.ingestion.deduplication.blockingschemes

import de.hpi.ingestion.datalake.models.Subject

/**
  * This trait is the template for our blocking schemes.
  */
trait BlockingScheme extends Serializable {
    val undefinedValue = "undefined"
    var tag = "BlockingScheme"
    var inputAttributes: List[String] = List[String]()

    /**
      * Gets the attributes to generate the key with as input and converts them to a list
      * if required for the blocking scheme.
      * @param attrList Sequence of attributes to use.
      */
    def setAttributes(attrList: String*): Unit = inputAttributes = attrList.toList

    /**
      * Generates key from the subject's properties.
      * @param subject Subject to use.
      * @return Key as list of strings.
      */
    def generateKey(subject: Subject): List[String]
}
