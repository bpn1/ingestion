package de.hpi.ingestion.datalake

import de.hpi.ingestion.datalake.mock.Entity
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TestData {
	val normalizationMapping = Map(
		"rootKey" -> List("root_value"),
		"nestedKey1" -> List("nested_value:1.1", "nested_value:1.2", "nested_value:1.3"),
		"nestedKey2" -> List("nested_value:2.1")
	)

	val testEntity = Entity(
		"test_key",
		Map(
			"nested_value:1.1" -> List("test_value:1.1"),
			"nested_value:1.2" -> List("test_value:1.2.1", "test_value:1.2.2")
		)
	)

	val propertyMapping = Map(
		"rootKey" -> List("test_key"),
		"nestedKey1" -> List("test_value:1.1", "test_value:1.2.1", "test_value:1.2.2")
	)
}
