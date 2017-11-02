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

package de.hpi.ingestion.datalake.models

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class VersionTest extends FlatSpec with SharedSparkContext with Matchers {

    "Default version" should "have the default values set" in {
        val programName = "VersionTest"
        val version = Version(programName, Nil, sc, false, None)
        version.version should not be null
        version.program shouldBe programName
        version.value shouldBe empty
        version.validity shouldBe empty
        version.datasources shouldBe empty
        version.timestamp should not be null
    }

    "Program name" should "have a timestamp appended" in {
        val programName = "VersionTest"
        val version = Version(programName, Nil, sc, false, None)
        version.program shouldEqual programName
        val versionWithTime = Version(programName, Nil, sc, true, None)
        versionWithTime.program should startWith (programName)
        versionWithTime.program should endWith (version.timestamp.toString)
    }
}
