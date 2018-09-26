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

package de.hpi.ingestion.curation

import java.util.UUID

import de.hpi.ingestion.datalake.models.{Subject, Version}
import org.apache.spark.SparkContext
import play.api.libs.json.{JsValue, Json}

// scalastyle:off line.size.limit
// scalastyle:off method.length
object TestData {

    def version(sc: SparkContext): Version = {
        Version("SomeTestApp", Nil, sc, false, None)
    }

    def commitJSON: String = {
        "{\"created\":{\"6a7b2436-255e-447f-8740-f7d353560cc3\":{\"name\":\"Test ag\",\"id\":\"6a7b2436-255e-447f-8740-f7d353560cc3\",\"properties\":{}, \"relations\":{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"owner\":\"\"}}}},\"updated\":{\"25486e12-be2f-4ba0-b498-94ffcd984528\":{\"master\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"id\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"datasource\":\"master\",\"name\":\"The Swan Inn\",\"aliases\":null,\"category\":\"business\",\"properties\":{\"geo_coords\":\"51.315404;0.891722\",\"id_wikidata\":\"Q26606155\",\"geo_country\":\"Vereinigtes Königreich\"},\"relations\":{\"6c37910e-7e7d-43f8-b537-683594a7517b\":{\"master\":\"1.0\"},\"74780dce-9bf3-4d3d-9f0d-750846d8f4cb\":{\"country\":null}}}},\"deleted\":{\"3254650b-269e-4d20-bb2b-48ee44013c88\":{\"master\":\"3254650b-269e-4d20-bb2b-48ee44013c88\",\"id\":\"3254650b-269e-4d20-bb2b-48ee44013c88\",\"datasource\":\"master\",\"name\":\"Deutschland AG\",\"aliases\":null,\"category\":\"business\",\"properties\":{\"gen_legal_form\":[\"AG\"],\"id_dbpedia\":[\"Deutschland AG\"],\"id_wikidata\":[\"Q1206257\"],\"id_wikipedia\":[\"Deutschland AG\"]},\"relations\":{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"master\":\"1.0\"}},\"selected\":true}}}"
    }

    def relationJSON: Map[String, JsValue] = {
        val json = "{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"owner\":\"\",\"owned by\":null},\"6a7b2436-255e-447f-8740-f7d353560cc3\":{\"slave\":\"1.0\"}}"
        Json.parse(json).as[Map[String, JsValue]]
    }

    def createJSON: JsValue = {
        val json = "{\"name\":\"Test ag\",\"id\":\"6a7b2436-255e-447f-8740-f7d353560cc3\",\"properties\":{\"geo_country\":\"Vereinigtes Königreich\"}, \"relations\":{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"owner\":\"\"}}}"
        Json.parse(json)
    }

    def createdSubject: Subject = {
        Subject(
            id = null,
            master = UUID.fromString("6a7b2436-255e-447f-8740-f7d353560cc3"),
            datasource = "human",
            name = Option("Test ag"),
            properties = Map("geo_country" -> List("Vereinigtes Königreich")),
            relations = Map(
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa87c") -> Map("owner" -> ""),
                UUID.fromString("6a7b2436-255e-447f-8740-f7d353560cc3") -> Map("slave" -> "1.0")
            )
        )
    }

    def subjectUpdate: List[(Subject, JsValue)] = {
        val oldSubject = Subject(
            id = UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528"),
            master = UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528"),
            datasource = "master",
            name = Option("The Swan Inn"),
            category = Option("business"),
            properties = Map(
                "geo_coords" -> List("51.315404;0.891722"),
                "id_wikidata" -> List("Q26606155")
            ),
            relations = Map(
                UUID.fromString("6c37910e-7e7d-43f8-b537-683594a7517b") -> Map("master" -> "1.0"),
                UUID.fromString("74780dce-9bf3-4d3d-9f0d-750846d8f4cb") -> Map("country" -> "")
            )
        )
        List(
            (oldSubject, Json.parse("{\"master\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"id\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"datasource\":\"master\",\"name\":\"The Swan Inn\",\"aliases\":null,\"category\":\"organization\",\"properties\":{\"geo_coords\":\"51.315404;0.891722\",\"id_wikidata\":\"Q26606155\",\"geo_country\":\"Vereinigtes Königreich\"},\"relations\":{\"6c37910e-7e7d-43f8-b537-683594a7517b\":{\"master\":\"1.0\"},\"74780dce-9bf3-4d3d-9f0d-750846d8f4cb\":{\"country\":null}}}")),
            (oldSubject, Json.parse("{\"master\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"id\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"datasource\":\"master\",\"name\":\"The Swan Inn\",\"aliases\":[\"alias1\"],\"category\":\"business\",\"properties\":{\"geo_coords\":\"51.315404;0.891722\",\"id_wikidata\":\"Q26606155\",\"geo_country\":\"Vereinigtes Königreich\"},\"relations\":{\"6c37910e-7e7d-43f8-b537-683594a7517b\":{\"master\":\"1.0\"},\"74780dce-9bf3-4d3d-9f0d-750846d8f4cb\":{\"country\":null}}}")),
            (oldSubject, Json.parse("{\"master\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"id\":\"25486e12-be2f-4ba0-b498-94ffcd984528\",\"datasource\":\"master\",\"name\":\"The Swan Inn\",\"aliases\":null,\"category\":\"business\",\"properties\":{\"geo_coords\":\"51.315404;0.891722\",\"id_wikidata\":\"Q26606155\"},\"relations\":{\"6c37910e-7e7d-43f8-b537-683594a7517b\":{\"master\":\"1.0\"},\"74780dce-9bf3-4d3d-9f0d-750846d8f4cb\":{\"city\":null}}}"))
        )
    }

    def subjectUpdateTargetSlaves: Map[UUID, Set[UUID]] = {
        Map(
            UUID.fromString("74780dce-9bf3-4d3d-9f0d-750846d8f4cb") -> Set(UUID.fromString("74780dce-9bf3-4d3d-9f0d-750846d81337"))
        )
    }

    def updatedSubjects: List[Subject] = {
        List(
            Subject(
                id = null,
                master = UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528"),
                datasource = "human",
                category = Option("organization"),
                properties = Map(
                    "geo_country" -> List("Vereinigtes Königreich")
                ),
                relations = Map(
                    UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528") -> Map("slave" -> "1.0")
                )
            ),
            Subject(
                id = null,
                master = UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528"),
                datasource = "human",
                aliases = List("alias1"),
                properties = Map(
                    "geo_country" -> List("Vereinigtes Königreich")
                ),
                relations = Map(
                    UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528") -> Map("slave" -> "1.0")
                )
            ),
            Subject(
                id = null,
                master = UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528"),
                datasource = "human",
                relations = Map(
                    UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528") -> Map("slave" -> "1.0"),
                    UUID.fromString("74780dce-9bf3-4d3d-9f0d-750846d81337") -> Map("city" -> "")
                )
            )
        )
    }

    def deletedSubject(masterId: UUID): Subject = {
        Subject(
            id = UUID.fromString("3254650b-269e-4d20-bb2b-48ee44013c88"),
            master = masterId,
            datasource = "human",
            properties = Map("deleted" -> List("true")),
            relations = Map(masterId -> Map("slave" -> "1.0"))
        )
    }

    def extractedRelations: Map[UUID, Map[String, String]] = {
        Map(
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa87c") -> Map("owner" -> "", "owned by" -> ""),
            UUID.fromString("6a7b2436-255e-447f-8740-f7d353560cc3") -> Map("slave" -> "1.0")
        )
    }

    def subjects: List[Subject] = {
        List(
            Subject.master(UUID.fromString("3254650b-269e-4d20-bb2b-48ee44013c88")).copy(
                name = Option("Deutschland AG"),
                category = Option("business"),
                properties = Map(
                    "gen_legal_form" -> List("AG"),
                    "id_dbpedia" -> List("Deutschland AG"),
                    "id_wikidata" -> List("Q1206257"),
                    "id_wikipedia" -> List("Deutschland AG")
                ),
                relations = Map(UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa87c") -> Map("master" -> "1.0"))
            ),
            Subject.master(UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528")).copy(
                name = Option("The Swann Inn"),
                category = Option("business"),
                relations = Map(UUID.fromString("6c37910e-7e7d-43f8-b537-683594a7517b") -> Map("master" -> "1.0"))
            ),
            Subject(
                id = UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984529"),
                master = UUID.fromString("25486e12-be2f-4ba0-b498-94ffcd984528"),
                datasource = "implisense",
                name = Option("The Swann Inn"),
                category = Option("business"),
                relations = Map(UUID.fromString("6c37910e-7e7d-43f8-b537-683594a7517b") -> Map("master" -> "1.0"))
            )
        )
    }

    def aliasJSON: JsValue = {
        val json = "{\"aliases\":[\"alias1\",\"alias2\",\"alias3\"]}"
        Json.parse(json)
    }

    def extractedAliases: List[String] = {
        List("alias1", "alias2", "alias3")
    }

    def propertyJSON: JsValue = {
        val json = "{\"date_founding\":\"+2003 07 00T00:00:00Z\",\"gen_employees\":\"6000; 12000; 13058; 10161; 2964; 5859; 1417; 899; 514\",\"gen_founder\":\"Elon Musk; Jeffrey B. Straubel; Marc Tarpenning; Martin Eberhard\",\"gen_legal_form\":\"Publikumsgesellschaft; Incorporated\",\"gen_urls\":\"http://www.teslamotors.com\",\"geo_city\":\"Palo Alto\",\"geo_coords\":\"1.1;2.2; 3.3;4.4\"}"
        Json.parse(json)
    }

    def extractedProperties: Map[String, List[String]] = {
        Map(
            "date_founding" -> List("+2003 07 00T00:00:00Z"),
            "gen_employees" -> List("6000", "12000", "13058", "10161", "2964", "5859", "1417", "899", "514"),
            "gen_founder" -> List("Elon Musk", "Jeffrey B. Straubel", "Marc Tarpenning", "Martin Eberhard"),
            "gen_legal_form" -> List("Publikumsgesellschaft", "Incorporated"),
            "gen_urls" -> List("http://www.teslamotors.com"),
            "geo_city" -> List("Palo Alto"),
            "geo_coords" -> List("1.1;2.2", "3.3;4.4")
        )
    }

    def relationsToMasters: Map[UUID, Map[String, String]] = {
        Map(
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa800") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa810") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "", // slave 1
                "followed by" -> "" // slave 2
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa820") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "", // slave 2
                "followed by" -> "" // slave 3
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa830") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "", // slave 2
                "followed by" -> "" // new -> any slave
            )
        )
    }

    def relationSlaves: List[Subject] = {
        List(
            Subject.master().copy(
                datasource = "implisense",
                relations = Map(
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa801") -> Map(
                        "owns" -> "" // slave 1
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa811") -> Map(
                        "owns" -> "", // slave 1
                        "owned by" -> "" // slave 1
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa812") -> Map(
                        "followed by" -> "" // slave 2
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa821") -> Map(
                        "owns" -> "" // slave 1
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa831") -> Map(
                        "owns" -> "" // slave 1
                    )
                )
            ),
            Subject.master().copy(
                datasource = "wikidata",
                relations = Map(
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa801") -> Map(
                        "owned by" -> "" // slave 1
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa812") -> Map(
                        "followed by" -> "123" // slave 2
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa822") -> Map(
                        "owned by" -> "" // slave 2
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa823") -> Map(
                        "followed by" -> "" // slave 3
                    ),
                    UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa832") -> Map(
                        "owned by" -> "" // slave 2
                    )
                )
            )
        )
    }

    def relationTargetSlaves: Map[UUID, Set[UUID]] = {
        Map(
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa800") -> Set(
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa801")
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa810") -> Set(
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa811"),
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa812")
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa820") -> Set(
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa821"),
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa822"),
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa823")
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa830") -> Set(
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa832"),
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa831")
            )
        )
    }

    def redirectedRelations: Map[UUID, Map[String, String]] = {
        Map(
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa801") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa811") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa812") -> Map(
                "followed by" -> "" // slave 2
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa821") -> Map(
                "owns" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa822") -> Map(
                "owned by" -> "" // slave 2
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa823") -> Map(
                "followed by" -> "" // slave 3
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa831") -> Map(
                "owns" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa832") -> Map(
                "owned by" -> "", // slave 2
                "followed by" -> "" // new -> any slave
            )
        )
    }

    def redirectExcludedRelations: Map[UUID, Map[String, String]] = {
        Map(
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa801") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa811") -> Map(
                "owns" -> "", // slave 1
                "owned by" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa812") -> Map(
                "followed by" -> "" // slave 2
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa821") -> Map(
                "owns" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa822") -> Map(
                "owned by" -> "" // slave 2
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa823") -> Map(
                "followed by" -> "" // slave 3
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa831") -> Map(
                "owns" -> "" // slave 1
            ),
            UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa832") -> Map(
                "owned by" -> "", // slave 2
                "followed by" -> "" // new -> any slave
            )
        )
    }

    def jsonPropertiesToExtract: List[JsValue] = {
        List(
            Json.parse("\"value; value\""),
            Json.parse("[\"value 1\", \"value 2\"]"),
            Json.parse("\"value 3\""),
            Json.parse("[\"name\", \"name, 2\"]"),
            Json.parse("\"12;13; 23;24; 45;56\"")
        )
    }

    def extractedJSONProperties: List[List[String]] = {
        List(
            List("value", "value"),
            List("value 1", "value 2"),
            List("value 3"),
            List("name", "name, 2"),
            List("12;13", "23;24", "45;56")
        )
    }
}
// scalastyle:on line.size.limit
// scalastyle:on method.length
