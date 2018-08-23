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

package de.hpi.ingestion.datalake

import java.util.UUID

import de.hpi.ingestion.datalake.mock.Entity
import de.hpi.ingestion.datalake.models.{ExtractedRelation, ImportRelation, Subject, Version}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// scalastyle:off line.size.limit
// scalastyle:off method.length
object CSVExportTestData {
    val datasource = "testSource"
    def subjects: List[Subject] = {
        List(
            Subject(
                id = UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130"),
                master = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"),
                datasource = datasource,
                name = Option("Name 1"),
                aliases = List("Alias 1", "Alias 1.1\""),
                category = None,
                properties = Map(),
                relations = Map(
                    UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d") -> Map("owned by" -> "")
                )
            ),
            Subject(
                id = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"),
                master = UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d"),
                datasource = datasource,
                name = Option("Name 2"),
                aliases = List("Alias 2"),
                category = Option("Category 2"),
                properties = Map("geo_city" -> List("value 1", "value 2")),
                relations = Map(
                    UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130") -> Map("owns" -> "", "parent organization" -> ""),
                    UUID.fromString("831f2c54-33d5-43fc-a515-d871946a655d") -> Map("owned by" -> "")
                )
            )
        )
    }

    def subjectNodeCSV: List[String] = {
        List(
            "\"4fbc0340-4862-431f-9c28-a508234b8130\",\"Name 1\",\"Alias 1;Alias 1.1\\\"\",\"\",\"\"," + propertyCSV.head,
            "\"831f2c54-33d5-43fc-a515-d871946a655d\",\"Name 2\",\"Alias 2\",\"Category 2\",\"\"," + propertyCSV.last
        )
    }

    def masterNodeCSV: List[String] = {
        List(
            "\"831f2c54-33d5-43fc-a515-d871946a655d\",\"Name 2\",\"Alias 2\",\"Category 2\",\"\"," + propertyCSV.last
        )
    }

    def subjectEdgeCSV: List[String] = {
        List(
            "4fbc0340-4862-431f-9c28-a508234b8130,831f2c54-33d5-43fc-a515-d871946a655d,owned by",
            "831f2c54-33d5-43fc-a515-d871946a655d,4fbc0340-4862-431f-9c28-a508234b8130,owns",
            "831f2c54-33d5-43fc-a515-d871946a655d,831f2c54-33d5-43fc-a515-d871946a655d,owned by"
        )
    }

    def masterEdgeCSV: List[String] = {
        List(
            "831f2c54-33d5-43fc-a515-d871946a655d,4fbc0340-4862-431f-9c28-a508234b8130,owns",
            "831f2c54-33d5-43fc-a515-d871946a655d,831f2c54-33d5-43fc-a515-d871946a655d,owned by"
        )
    }

    private def toCSV(properties: List[(String, List[String])]): String = {
        properties.map {
            case (_, values) if values.isEmpty => ""
            case (_, values) => values.mkString(";")
        }.map(property => s""""$property"""")
        .mkString(",")
    }

    def propertyNames: List[String] = Subject.normalizedPropertyKeyList.sorted

    def emptyPropertyCSV: String = toCSV(propertyNames.map(name => (name, Nil)))

    def propertyCSV: List[String] = {
        val subjectProperties = Map("geo_city" -> List("value 1", "value 2"))
        val subjectCSV = toCSV(propertyNames.map(name => (name, subjectProperties.getOrElse(name, Nil))))
        List(emptyPropertyCSV, subjectCSV)
    }
}
// scalastyle:on line.size.limit
// scalastyle:on method.length
