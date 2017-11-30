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

package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.textmining.models.TrieAliasArticle
import play.api.libs.json.{JsObject, Json}

import scala.io.Source

// scalastyle:off line.size.limit
object TestData {
    def spiegelFile(): List[String] = {
        Source.fromURL(getClass.getResource("/spiegel/spiegel.json"))
            .getLines()
            .toList
    }

    def spiegelJson(): List[JsObject] = {
        spiegelFile()
            .map(Json.parse)
            .map(_.as[JsObject])
    }

    def spiegelPages(): List[String] = {
        List(
            """<div class="spArticleContent">test tag 1</div> abc""",
            """<div class="dig-artikel">test tag 2</div> abc""",
            """<div class="article-section">test tag 3</div> abc""",
            """test no tag"""
        )
    }

    def pageTexts(): List[String] = {
        List(
            "test tag 1",
            "test tag 2",
            "test tag 3",
            "test no tag"
        )
    }

    def parsedArticles(): List[TrieAliasArticle] = {
        List(
            TrieAliasArticle(
                id = "spiegel id 1",
                title = Option("test title 1"),
                text = Option("test title 1 test body 1")
            ),
            TrieAliasArticle(
                id = "spiegel id 2",
                title = Option("test title 2"),
                text = Option("test body 2")
            ),
            TrieAliasArticle(
                id = "spiegel id 3",
                title = Option("test title 3"),
                text = Option("test title 3")
            ),
            TrieAliasArticle(
                id = "spiegel id 4",
                title = None,
                text = Option("abc")
            ),
            TrieAliasArticle(
                id = "spiegel id 5",
                title = Option("test title 5"),
                text = None)
        )
    }
}
// scalastyle:on line.size.limit
