package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.models.Version
import org.scalatest.{FlatSpec, Matchers}

class WikiDataDataLakeImportTest extends FlatSpec with SharedSparkContext with Matchers {

	"WikidataEntity" should "be translated into a Subject" in {
		val subjects = TestData.completeWikidataEntities()
		    .map(WikiDataDataLakeImport.translateToSubject(_, Version("WikiDataDataLakeImportTest", Nil, sc)))
		    .map(subject => (subject.name, subject.aliases, subject.category, subject.properties))
		val expectedSubjects = TestData.translatedSubjects()
			.map(subject => (subject.name, subject.aliases, subject.category, subject.properties))
		subjects shouldBe expectedSubjects
	}
}
