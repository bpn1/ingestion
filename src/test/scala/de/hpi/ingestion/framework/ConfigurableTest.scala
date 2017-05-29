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
	}

	they should "be parsed when the respective getters are called and configFile is set" in {
		val configurable = new MockConfigurable
		configurable.configFile = ""
		configurable.settings shouldBe empty

		configurable.configFile = "test.xml"
		configurable.settings should not be empty
		configurable.settings shouldEqual TestData.parsedSettings
		configurable.scoreConfigSettings should not be empty
		configurable.scoreConfigSettings shouldEqual TestData.parsedScoreConfig
	}

	"Settings" should "be parsed" in {
		val configurable = new MockConfigurable
		val settings = configurable.parseSettings(TestData.configXML)
		val expectedSettings = TestData.parsedSettings
		settings shouldEqual expectedSettings
	}

	it should "return an empty map if there is no settings node" in {
		val configurable = new MockConfigurable()
		val settings = configurable.parseSettings(TestData.configWithouSettingsXML)
		settings shouldBe empty
	}

	"Sim Measures" should "be parsed" in {
		val configurable = new MockConfigurable
		val config = configurable.parseSimilarityMeasures(TestData.configXML)
		val expectedConfig = TestData.parsedScoreConfig
		config shouldEqual expectedConfig
	}
}
