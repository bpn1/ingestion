package de.hpi.ingestion.datalake

import de.hpi.ingestion.datalake.mock.Import
import org.scalatest.{FlatSpec, Matchers}

class DataLakeImportUnitTest extends FlatSpec with Matchers {
	"parseNormalizationConfig" should "generate a normalization mapping from a given path" in {
		val mapping = Import.parseNormalizationConfig(Import.normalizationFile)
		val expected = TestData.normalizationMapping
		mapping shouldEqual expected
	}

	"normalizeProperties" should "normalize the properties of an entity" in {
		val entity = TestData.testEntity
		val mapping = TestData.normalizationMapping
		val properties = Import.normalizeProperties(entity, mapping)
		val expected = TestData.propertyMapping
		properties shouldEqual expected
	}
}



