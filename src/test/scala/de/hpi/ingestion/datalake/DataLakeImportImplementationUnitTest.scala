package de.hpi.ingestion.datalake

import de.hpi.ingestion.datalake.mock.Import
import org.scalatest.{FlatSpec, Matchers}

class DataLakeImportImplementationUnitTest extends FlatSpec with Matchers {
//	"parseConfig" should "set the parsed configuration as settings" in {
//		val expected = TestData.configMapping
//		Import.parseConfig("configs/datalake_import.xml")
//		Import.settings shouldEqual expected
//	}

	"parseNormalizationConfig" should "generate a normalization mapping from a given path" in {
		val mapping = Import.parseNormalizationConfig(Import.normalizationFile)
		val expected = TestData.normalizationMapping
		mapping shouldEqual expected
	}

	"normalizeProperties" should "normalize the properties of an entity" in {
		val entity = TestData.testEntity
		val mapping = TestData.normalizationMapping
		val strategies = TestData.strategyMapping
		val properties = Import.normalizeProperties(entity, mapping, strategies)
		val expected = TestData.propertyMapping
		properties shouldEqual expected
	}
}
