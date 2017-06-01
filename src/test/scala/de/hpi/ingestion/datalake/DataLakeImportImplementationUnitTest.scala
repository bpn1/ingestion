package de.hpi.ingestion.datalake

import de.hpi.ingestion.datalake.mock.Import
import org.scalatest.{FlatSpec, Matchers}

class DataLakeImportImplementationUnitTest extends FlatSpec with Matchers {
	"normalizeProperties" should "normalize the properties of an entity" in {
		val entity = TestData.testEntity
		val mapping = TestData.normalizationMapping
		val strategies = TestData.strategyMapping
		val properties = Import.normalizeProperties(entity, mapping, strategies)
		val expected = TestData.propertyMapping
		properties shouldEqual expected
	}
}
