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

package de.hpi.ingestion.deduplication.similarity

import org.scalatest.{FlatSpec, Matchers}

class UrlCompareUnitTest extends FlatSpec with Matchers {
    "cleanUrlString" should "return the host name of a given url" in {
        val computed = UrlCompare.cleanUrlString("http://some.neckermann.berlin/some/stuff")
        val expected = "some.neckermann.berlin"
        computed shouldEqual expected
    }

    "UrlCompare" should "compare two hostnames with JaroWinkler" in {
        val computed1 = UrlCompare.compare("http://hpi.de", "https://www.hpi.de/callmefelix")
        val computed2 = UrlCompare.compare("http://augenbluten.de", "https://downtown.veitimwinkel.de")

        val expected1 = JaroWinkler.compare("hpi.de","hpi.de")
        val expected2 = JaroWinkler.compare("augenbluten.de","downtown.veitimwinkel.de")

        computed1 shouldEqual expected1
        computed2 shouldEqual expected2
    }
}
