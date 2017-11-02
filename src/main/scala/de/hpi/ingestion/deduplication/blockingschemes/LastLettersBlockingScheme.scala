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
  * This class uses the last five characters of the name property as key.
  */
class LastLettersBlockingScheme extends BlockingScheme {
    tag = "LastLettersBlockingScheme"
    override def generateKey(subject: Subject): List[String] = {
        subject.name.map { name =>
            val start = Math.max(0, name.length - 5)
            List(name.substring(start, name.length).toLowerCase)
        }.getOrElse(List(undefinedValue))
    }
}

/**
  * Companion object adding an easy to use constructor via apply.
  */
object LastLettersBlockingScheme {
    /**
      * Returns a Last Letters Blocking Scheme with the given tag.
      * @param tag tag to use
      * @return Last Letters Blocking Schemes with the given tag
      */
    def apply(tag: String = "LastLettersBlockingScheme"): LastLettersBlockingScheme = {
        val scheme = new LastLettersBlockingScheme
        scheme.tag = tag
        scheme
    }
}
