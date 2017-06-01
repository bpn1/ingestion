package de.hpi.ingestion.datalake

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.mock.{MockImport, MockSubjectImport}
import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class DataLakeImportImplementationUnitTest extends FlatSpec with Matchers with SharedSparkContext {
	"normalizeProperties" should "normalize the properties of an entity" in {
		val entity = TestData.testEntity
		val mapping = TestData.normalizationMapping
		val strategies = TestData.strategyMapping
		val properties = MockImport.normalizeProperties(entity, mapping, strategies)
		val expected = TestData.propertyMapping
		properties shouldEqual expected
	}

	"Entities" should "be translated" in {
		val input = List(sc.parallelize(TestData.translationEntities)).toAnyRDD()
		val subjects = MockSubjectImport.run(input, sc).fromAnyRDD[Subject]().head.collect.toList
		val expectedSubjects = TestData.translatedSubjects
		subjects shouldEqual expectedSubjects
	}
}
