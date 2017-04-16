package de.hpi.ingestion.datalake.models

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class VersionTest extends FlatSpec with SharedSparkContext with Matchers {

	"Default version" should "have the default values set" in {
		val programName = "VersionTest"
		val version = Version(programName, List[String](), sc)
		version.version should not be null
		version.program shouldBe programName
		version.value shouldBe empty
		version.validity shouldBe empty
		version.datasources shouldBe empty
		version.timestamp should not be null
	}
}