package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.Cooccurrence
import org.scalatest.{FlatSpec, Matchers}

class CooccurrenceCounterTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {
	"All entity lists" should "be extracted" in {
		val sentences = TestData.sentenceList()
		val entityList = sentences.map(CooccurrenceCounter.sentenceToEntityList)
		entityList shouldEqual TestData.entityLists()
	}


	"Found cooccurrences" should "exactly these cooccurrences" in {
		val sentences = sc.parallelize(TestData.sentencesWithCooccurrences())
		val cooccurrences = CooccurrenceCounter.run(List(sentences).toAnyRDD(), sc)
			.fromAnyRDD[Cooccurrence]()
			.head
		val expectedCooccurrences = sc.parallelize(TestData.cooccurrences())
		assertRDDEquals(cooccurrences, expectedCooccurrences)
	}
}
