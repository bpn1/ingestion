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

package de.hpi.ingestion.dataimport.wikipedia

import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import scala.io.Source

object TestData {

    def pageXML: List[String] = {
        Source.fromURL(getClass.getResource("/wikipedia/raw_pages.xml")).getLines.toList
    }

    def wikipediaEntries: List[WikipediaEntry] = {
        List(
            WikipediaEntry("Alan Smithee", Option("Text 1")),
            WikipediaEntry("Actinium", Option("Text 2")),
            WikipediaEntry("Ang Lee", Option("Text 3")),
            WikipediaEntry("Anschluss (Soziologie)", Option("Text 4")),
            WikipediaEntry("Anschlussfähigkeit", Option("Text 5")))
    }
}

