package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.models.Subject
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class DBpediaRelationImportTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"Relations" should "be added" in {
		val dbpedia = sc.parallelize(TestData.dbpedia)
		val relations = sc.parallelize(TestData.dbpediaRelations)
		val input = List(dbpedia).toAnyRDD() ::: List(relations).toAnyRDD()
		val output = DBpediaRelationImport
			.run(input, sc)
			.fromAnyRDD[Subject]()
			.head
			.collect
			.toList
			.sortBy(_.id)

		val expected =  TestData
			.dbpediaImportedRelations
			.sortBy(_.id)

		output should have length expected.length
		(output, expected).zipped.foreach { case (subject, expectedSubject) =>
			subject.relations shouldEqual expectedSubject.relations
		}
	}
}
