package de.hpi.ingestion.textmining.models

import org.scalatest.{FlatSpec, Matchers}

class ProtoFeatureEntryTest extends FlatSpec with Matchers {
	"Proto Feature Entry" should "be transformed into a Feature Entry" in {
		val entries = TestData.protoFeatureEntries()
			.map { case (entry, page, cosSim) => entry.toFeatureEntry(page, cosSim)}
		val expectedEntries = TestData.featureEntries()
		entries shouldEqual expectedEntries
	}
}
