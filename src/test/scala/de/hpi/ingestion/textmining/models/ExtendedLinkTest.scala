package de.hpi.ingestion.textmining.models

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class ExtendedLinkTest extends FlatSpec with SharedSparkContext with Matchers {
	"Filtered Extended Link Pages" should "be exactly these pages" in {
		val extendedLinks = TestData.edgeCaseExtendedLinks()
		val filteredExtendedLinks = extendedLinks.map(_.filterExtendedLink(1, 0.1))
		filteredExtendedLinks shouldEqual TestData.edgeCaseFilteredExtendedLinkPages()
	}
}
