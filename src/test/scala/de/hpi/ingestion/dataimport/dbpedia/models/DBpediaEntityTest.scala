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

package de.hpi.ingestion.dataimport.dbpedia.models

import org.scalatest.{FlatSpec, Matchers}

class DBpediaEntityTest extends FlatSpec with Matchers {
    "Attribute values" should "be returned" in {
        val id = "Q1"
        val name = "name"
        val sub1 = DBpediaEntity(dbpedianame = id, label = Option(name))
        sub1.get("dbpedianame") shouldBe List(id)
        sub1.get("label") shouldEqual List(name)
        sub1.get("description") shouldBe empty
        sub1.get("data") shouldBe empty
        sub1.get("category") shouldBe empty
    }
}
