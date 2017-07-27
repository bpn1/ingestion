package de.hpi.ingestion.dataimport.kompass

import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.implicits.CollectionImplicits._

class KompassDataLakeImportTest extends FlatSpec with Matchers with SharedSparkContext {
	"extractAddress" should "extract and normalize street, postal, city and country from the address" in {
		val addresses = TestData.unnormalizedAddresses
		val normalizedAddresses = addresses.map(KompassDataLakeImport.extractAddress)
		val expected = TestData.normalizedAddresses
		normalizedAddresses shouldEqual expected
	}
}
