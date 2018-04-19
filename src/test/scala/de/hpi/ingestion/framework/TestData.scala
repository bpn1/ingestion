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

package de.hpi.ingestion.framework

import de.hpi.ingestion.deduplication.models.config.{AttributeConfig, SimilarityMeasureConfig}
import de.hpi.ingestion.deduplication.similarity.{ExactMatchString, JaroWinkler, MongeElkan}

import scala.xml.{Node, XML}

// scalastyle:off line.size.limit
object TestData {

    def parsedScoreConfig: List[AttributeConfig] = List(
        AttributeConfig(
            "name",
            1.0,
            List(
                SimilarityMeasureConfig(similarityMeasure = MongeElkan, weight = 0.5333),
                SimilarityMeasureConfig(similarityMeasure = JaroWinkler, weight = 0.4667)
            )
        )
    )

    def parsedScoreConfigWithoutWeights: List[AttributeConfig] = List(
        AttributeConfig(
            "name",
            0.5,
            List(
                SimilarityMeasureConfig(similarityMeasure = MongeElkan, weight = 0.5),
                SimilarityMeasureConfig(similarityMeasure = JaroWinkler, weight = 0.5)
            )
        ),
        AttributeConfig(
            "category",
            0.5,
            List(
                SimilarityMeasureConfig(similarityMeasure = ExactMatchString, weight = 1.0)
            )
        )
    )

    def parsedSettings: Map[String, String] = {
        Map(
            "key1" -> "val 1",
            "key2" -> "val 2",
            "key3" -> "val 3",
            "key4" -> "val 4")
    }

    def configXML: Node = {
        XML.load(getClass.getResource("/framework/test.xml"))
    }

    def configWithoutSettingsXML: Node = {
        XML.load(getClass.getResource("/framework/test2.xml"))
    }

    def configsWithoutWeightsXML: Node = {
        XML.load(getClass.getResource("/framework/test3.xml"))
    }

    def importConfigXML: Node = {
        XML.load(getClass.getResource("/datalake/normalization.xml"))
    }

    def normalizationSettings: Map[String, List[String]] = {
        Map(
            "rootKey" -> List("root_value"),
            "nestedKey1" -> List("nested_value:1.1", "nested_value:1.2", "nested_value:1.3"),
            "nestedKey2" -> List("nested_value:2.1")
        )
    }

    def sectorSettings: Map[String, List[String]] = {
        Map(
            "Category 1" -> List("value1.1", "value1.2"),
            "Category 2" -> List("value2.1"),
            "Category 3" -> List("value3.1", "value3.2")
        )
    }

    def commitJson: String = {
        "{\"created\":{\"6a7b2436-255e-447f-8740-f7d353560cc3\":{\"name\":\"Test ag\",\"id\":\"6a7b2436-255e-447f-8740-f7d353560cc3\",\"properties\":{}}},\"updated\":{},\"deleted\":{\"3254650b-269e-4d20-bb2b-48ee44013c88\":{\"master\":\"3254650b-269e-4d20-bb2b-48ee44013c88\",\"id\":\"3254650b-269e-4d20-bb2b-48ee44013c88\",\"datasource\":\"master\",\"name\":\"Deutschland AG\",\"aliases\":null,\"category\":\"business\",\"properties\":{\"gen_legal_form\":[\"AG\"],\"id_dbpedia\":[\"Deutschland AG\"],\"id_wikidata\":[\"Q1206257\"],\"id_wikipedia\":[\"Deutschland AG\"]},\"relations\":{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"master\":\"1.0\"}},\"selected\":true}}}"
    }

    def base64Commit: String = {
        "eyJjcmVhdGVkIjp7IjZhN2IyNDM2LTI1NWUtNDQ3Zi04NzQwLWY3ZDM1MzU2MGNjMyI6eyJuYW1lIjoiVGVzdCBhZyIsImlkIjoiNmE3YjI0MzYtMjU1ZS00NDdmLTg3NDAtZjdkMzUzNTYwY2MzIiwicHJvcGVydGllcyI6e319fSwidXBkYXRlZCI6e30sImRlbGV0ZWQiOnsiMzI1NDY1MGItMjY5ZS00ZDIwLWJiMmItNDhlZTQ0MDEzYzg4Ijp7Im1hc3RlciI6IjMyNTQ2NTBiLTI2OWUtNGQyMC1iYjJiLTQ4ZWU0NDAxM2M4OCIsImlkIjoiMzI1NDY1MGItMjY5ZS00ZDIwLWJiMmItNDhlZTQ0MDEzYzg4IiwiZGF0YXNvdXJjZSI6Im1hc3RlciIsIm5hbWUiOiJEZXV0c2NobGFuZCBBRyIsImFsaWFzZXMiOm51bGwsImNhdGVnb3J5IjoiYnVzaW5lc3MiLCJwcm9wZXJ0aWVzIjp7Imdlbl9sZWdhbF9mb3JtIjpbIkFHIl0sImlkX2RicGVkaWEiOlsiRGV1dHNjaGxhbmQgQUciXSwiaWRfd2lraWRhdGEiOlsiUTEyMDYyNTciXSwiaWRfd2lraXBlZGlhIjpbIkRldXRzY2hsYW5kIEFHIl19LCJyZWxhdGlvbnMiOnsiYzE3NzMyNmEtODg5OC00YmM3LThhY2EtYTA0MDgyNGFhODdjIjp7Im1hc3RlciI6IjEuMCJ9fSwic2VsZWN0ZWQiOnRydWV9fX0="
    }
}
// scalastyle:on line.size.limit
