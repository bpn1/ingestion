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

import de.hpi.ingestion.framework.mock.MockConfigurable
import org.scalatest.{FlatSpec, Matchers}

class ConfigurableTest extends FlatSpec with Matchers {

    "Config" should "be parsed" in {
        val configurable = new MockConfigurable
        configurable.parseConfig()
        configurable.settings should not be empty
        configurable.settings shouldEqual TestData.parsedSettings
        configurable.scoreConfigSettings should not be empty
        configurable.scoreConfigSettings shouldEqual TestData.parsedScoreConfig
        configurable.parseImportConfig()
        configurable.normalizationSettings should not be empty
        configurable.sectorSettings should not be empty
    }

    they should "be parsed when the respective getters are called and configFile is set" in {
        val configurable = new MockConfigurable
        configurable.configFile = ""
        configurable.settings shouldBe empty
        configurable.scoreConfigSettings shouldBe empty

        configurable.configFile = "test.xml"
        configurable.settings should not be empty
        configurable.settings shouldEqual TestData.parsedSettings
        configurable.scoreConfigSettings should not be empty
        configurable.scoreConfigSettings shouldEqual TestData.parsedScoreConfig

        configurable.importConfigFile = ""
        configurable.normalizationSettings shouldBe empty
        configurable.sectorSettings shouldBe empty

        configurable.importConfigFile = "normalization_wikidata.xml"
        configurable.normalizationSettings = Map()
        configurable.normalizationSettings should not be empty
        configurable.sectorSettings = Map()
        configurable.sectorSettings should not be empty

        configurable.importConfigFile = "src/test/resources/datalake/normalization.xml"
        configurable.normalizationSettings = Map()
        configurable.normalizationSettings should not be empty
        configurable.normalizationSettings shouldEqual TestData.normalizationSettings
        configurable.sectorSettings = Map()
        configurable.sectorSettings should not be empty
        configurable.sectorSettings shouldEqual TestData.sectorSettings
    }

    "Settings" should "be parsed" in {
        val configurable = new MockConfigurable
        val settings = configurable.parseSettings(TestData.configXML)
        val expectedSettings = TestData.parsedSettings
        settings shouldEqual expectedSettings
    }

    it should "return an empty map if there is no settings node" in {
        val configurable = new MockConfigurable
        val settings = configurable.parseSettings(TestData.configWithoutSettingsXML)
        settings shouldBe empty
    }

    "Score Settings" should "be parsed" in {
        val configurable = new MockConfigurable
        val config = configurable.parseSimilarityMeasures(TestData.configXML)
        val expectedConfig = TestData.parsedScoreConfig
        config shouldEqual expectedConfig
    }

    it should "equally filled with weights if none is given" in {
        val configurable = new MockConfigurable
        val config = configurable.parseSimilarityMeasures(TestData.configsWithoutWeightsXML)
        val expectedConfig = TestData.parsedScoreConfigWithoutWeights
        config shouldEqual expectedConfig
    }

    "Normalization settings" should "be parsed" in {
        val configurable = new MockConfigurable
        val config = configurable.parseNormalizationConfig(TestData.importConfigXML)
        val expectedConfig = TestData.normalizationSettings
        config shouldEqual expectedConfig
    }

    "Sector settings" should "be parsed" in {
        val configurable = new MockConfigurable
        val config = configurable.parseSectorConfig(TestData.importConfigXML)
        val expectedConfig = TestData.sectorSettings
        config shouldEqual expectedConfig
    }
}
