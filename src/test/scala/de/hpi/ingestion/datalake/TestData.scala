package de.hpi.ingestion.datalake

import de.hpi.ingestion.datalake.mock.Entity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TestData {
	def testEntity: Entity = Entity(
		"test_key",
		Map(
			"nested_value:1.1" -> List("test_value:1.1"),
			"nested_value:1.2" -> List("test_value:1.2.1", "test_value:1.2.2")
		)
	)

	def configMapping: Map[String, String] = Map(
		"outputKeyspace" -> "datalake",
		"outputTable" -> "subject_temp",
		"versionTable" -> "version"
	)

	def normalizationMapping: Map[String, List[String]] = Map(
		"rootKey" -> List("root_value"),
		"nestedKey1" -> List("nested_value:1.1", "nested_value:1.2", "nested_value:1.3"),
		"nestedKey2" -> List("nested_value:2.1")
	)

	def propertyMapping: Map[String, List[String]] = Map(
		"rootKey" -> List("test_key"),
		"nestedKey1" -> List("test_value:1.1", "test_value:1.2.1", "test_value:1.2.2")
	)
}
