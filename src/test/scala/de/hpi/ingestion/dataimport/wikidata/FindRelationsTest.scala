
package de.hpi.ingestion.dataimport.wikidata

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.datalake.models.Version
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class FindRelationsTest extends FlatSpec with SharedSparkContext with Matchers {
	"Subject relations" should "be found" in {
		val nameMap = TestData.resolvedNameMap()
		val subjects = TestData.unresolvedSubjects()
			.map(FindRelations.findRelations(_, nameMap, Version("FindRelationsTest", Nil, sc, false)))
			.map(subject => (subject.id, subject.name, subject.properties, subject.relations))
		val expectedSubjects = TestData.resolvedSubjects()
			.map(subject => (subject.id, subject.name, subject.properties, subject.relations))
		subjects shouldEqual expectedSubjects
	}

	"Name resolve map" should "contain all resolvable names" in {
		val subjects = sc.parallelize(TestData.unresolvedSubjects())
		val resolvedNames = FindRelations.resolvableNamesMap(subjects)
		val expectedMap = TestData.resolvedNameMap()
		resolvedNames shouldEqual expectedMap
	}

}
