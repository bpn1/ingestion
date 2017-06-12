package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.FeatureEntry

class SecondOrderFeatureGeneratorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Rank" should "be computed correctly" in {
		val values = TestData.valuesList()
		val ranks = SecondOrderFeatureGenerator.computeRanks(values)
		ranks shouldEqual TestData.ranksList()
	}

	it should "be computed correctly for a longer example" in {
		val values = TestData.longValuesList()
		val ranks = SecondOrderFeatureGenerator.computeRanks(values)
		ranks shouldEqual TestData.longRanksList()
	}

	"Difference to highest value (delta top)" should "be computed correctly" in {
		val values = TestData.valuesList()
		val deltaTops = SecondOrderFeatureGenerator.computeDeltaTopValues(values)
		deltaTops shouldEqual TestData.deltaTopValuesList()
	}

	"Difference to next value (delta successor)" should "be computed correctly" in {
		val values = TestData.valuesList()
		val deltaTops = SecondOrderFeatureGenerator.computeDeltaSuccessorValues(values)
		deltaTops shouldEqual TestData.deltaSuccessorValuesList()
	}

	"Feature entries with second order features" should "be exactly these feature entries" in {
		val featureEntries1 = sc.parallelize(TestData.featureEntriesList())
		val featureEntries2 = sc.parallelize(TestData.featureEntriesForSingleAliasList())
		val featureEntries3 = sc.parallelize(TestData.featureEntriesForManyPossibleEntitiesList())
		val input1 = List(featureEntries1).toAnyRDD()
		val input2 = List(featureEntries2).toAnyRDD()
		val input3 = List(featureEntries3).toAnyRDD()

		val featureEntriesWithSOF1 = SecondOrderFeatureGenerator.run(input1, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
			.toSet
		val featureEntriesWithSOF2 = SecondOrderFeatureGenerator.run(input2, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
			.toList
			.sortBy(featureEntry => (featureEntry.article, featureEntry.offset, featureEntry.entity))
		val featureEntriesWithSOF3 = SecondOrderFeatureGenerator.run(input3, sc)
			.fromAnyRDD[FeatureEntry]()
			.head
			.collect
			.toList
			.sortBy(featureEntry => (featureEntry.entity_score.rank, featureEntry.entity))

		featureEntriesWithSOF1 shouldEqual TestData.featureEntriesWitSOFSet()
		featureEntriesWithSOF2 shouldEqual TestData.featureEntriesForSingleAliasWithSOFList()
		featureEntriesWithSOF3 shouldEqual TestData.featureEntriesForManyPossibleEntitiesWithSOFList()
	}
}
