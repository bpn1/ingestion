package de.hpi.ingestion.datalake

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.mock.Import
import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class DataLakeImportImplementationUnitTest extends FlatSpec with Matchers with SharedSparkContext {
	"parseNormalizationConfig" should "generate a normalization mapping from a given path" in {
		val mapping = Import.parseNormalizationConfig(Import.normalizationFile)
		val expected = TestData.normalizationMapping
		mapping shouldEqual expected
	}

	"parseCategoryConfig" should "generate a category normalization mapping from a given path" in {
		val mapping = Import.parseCategoryConfig(Import.categoryConfigFile)
		val expected = TestData.categoryMapping
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

	"filterEntities" should "filter no element by default" in {
		val entities = TestData.testEntities
		val filteredEntities = entities.filter(Import.filterEntities)
		filteredEntities shouldEqual entities
	}

	"extractLegalForm" should "extract the legal form from a given name" in {
		val classifier = Import.classifier
		val companyNames = TestData.companyNames
		companyNames.foreach { case (name, expected) =>
			val legalForms = Import.extractLegalForm(name, classifier)
			legalForms shouldEqual expected
		}
	}

	"run" should "import a new datasource to the datalake" in {
		val input = TestData.input(sc)
		val output = Import.run(input, sc).fromAnyRDD[Subject]().head.collect
		val expected = TestData.output
		(output, expected).zipped.foreach { case (subject, expectedSubject) =>
			subject.name shouldEqual expectedSubject.name
		}
	}
}
