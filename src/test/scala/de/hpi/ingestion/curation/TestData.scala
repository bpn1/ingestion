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
object TestData {

    def version(sc: SparkContext): Version = {
        Version("SomeTestApp", Nil, sc, false, None)
    }

    def commitJson: String = {
        "{\"created\":{\"6a7b2436-255e-447f-8740-f7d353560cc3\":{\"name\":\"Test ag\",\"id\":\"6a7b2436-255e-447f-8740-f7d353560cc3\",\"properties\":{}, \"relations\":{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"owner\":\"\"}}}},\"updated\":{},\"deleted\":{\"3254650b-269e-4d20-bb2b-48ee44013c88\":{\"master\":\"3254650b-269e-4d20-bb2b-48ee44013c88\",\"id\":\"3254650b-269e-4d20-bb2b-48ee44013c88\",\"datasource\":\"master\",\"name\":\"Deutschland AG\",\"aliases\":null,\"category\":\"business\",\"properties\":{\"gen_legal_form\":[\"AG\"],\"id_dbpedia\":[\"Deutschland AG\"],\"id_wikidata\":[\"Q1206257\"],\"id_wikipedia\":[\"Deutschland AG\"]},\"relations\":{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"master\":\"1.0\"}},\"selected\":true}}}"
    }

    def relationJSON: Map[String, JsValue] = {
        val json = "{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"owner\":\"\",\"owned by\":null},\"6a7b2436-255e-447f-8740-f7d353560cc3\":{\"slave\":\"1.0\"}}"
        Json.parse(json).as[Map[String, JsValue]]
    }

    def createJSON: JsValue = {
        val json = "{\"name\":\"Test ag\",\"id\":\"6a7b2436-255e-447f-8740-f7d353560cc3\",\"properties\":{}, \"relations\":{\"c177326a-8898-4bc7-8aca-a040824aa87c\":{\"owner\":\"\"}}}"
        Json.parse(json)
    }

    def createdSubject(masterId: UUID): Subject = {
        Subject(
            id = UUID.fromString("6a7b2436-255e-447f-8740-f7d353560cc3"),
            master = masterId,
            datasource = "human",
            name = Option("Test ag"),
            relations = Map(
                UUID.fromString("c177326a-8898-4bc7-8aca-a040824aa87c") -> Map("owner" -> ""),
                masterId -> Map("slave" -> "1.0")
            )
        )
    }

    def updateJSON: JsValue = {
        val json = ""
        Json.parse(json)
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
            )
        )
    }
}
// scalastyle:on line.size.limit
