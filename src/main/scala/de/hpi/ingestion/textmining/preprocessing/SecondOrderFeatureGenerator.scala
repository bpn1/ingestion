/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.preprocessing

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Enriches page scores and cosine similarities of given `FeatureEntries` with their respective second order feature
  * values.
  */
class SecondOrderFeatureGenerator extends SparkJob {
	import SecondOrderFeatureGenerator._
	appName = "Second Order Feature Generator"
	configFile = "textmining.xml"

	var featureEntries: RDD[FeatureEntry] = _
	var featureEntriesWithSOF: RDD[FeatureEntry] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Feature Entries without second order features from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		featureEntries = sc.cassandraTable[FeatureEntry](settings("keyspace"), settings("featureTable"))
	}

	/**
	  * Saves the second-order-feature-enriched Feature Entries to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		featureEntriesWithSOF.saveToCassandra(settings("keyspace"), settings("secondOrderFeatureTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Enriches page scores and cosine similarities of given FeatureEntries with their respective second order feature
	  * values.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		featureEntriesWithSOF = featureEntries
			.map(featureEntry => ((featureEntry.article, featureEntry.offset), List(featureEntry)))
			.reduceByKey(_ ++ _)
			.flatMap { case (linkPosition, entries) =>
				val pageScores = entries.map(_.entity_score.value)
				val cosineSimilarities = entries.map(_.cosine_sim.value)
				entries
					.zip(generateMultiFeatures(pageScores))
					.zip(generateMultiFeatures(cosineSimilarities))
					.map { case ((featureEntry, extendedPageScore), extendedCosineSim) =>
						featureEntry.entity_score.rank = extendedPageScore.rank
						featureEntry.entity_score.delta_top = extendedPageScore.delta_top
						featureEntry.entity_score.delta_successor = extendedPageScore.delta_successor
						featureEntry.cosine_sim.rank = extendedCosineSim.rank
						featureEntry.cosine_sim.delta_top = extendedCosineSim.delta_top
						featureEntry.cosine_sim.delta_successor = extendedCosineSim.delta_successor
						featureEntry
					}
			}
	}
}

object SecondOrderFeatureGenerator {
	/**
	  * Computes the rank for each given value.
	  *
	  * @param values values
	  * @return ranks for the values (starting with 1 for highest value)
	  */
	def computeRanks(values: List[Double]): List[Int] = {
		var rank = 1
		val ranksMap = values
			.countElements()
			.toSeq
			.sortBy(_._1)(Ordering[Double].reverse)
			.map { case (value, count) =>
				val rankedValue = (value, rank)
				rank += count
				rankedValue
			}.toMap

		values.map(ranksMap)
	}

	/**
	  * Computes the (absolute) difference to the highest value for each given value.
	  *
	  * @param values values
	  * @return differences to the highest value (Double.PositiveInfinity for the highest value)
	  */
	def computeDeltaTopValues(values: List[Double]): List[Double] = {
		val maxValue = values.max
		values.map(value => if(value == maxValue) Double.PositiveInfinity else maxValue - value)
	}

	/**
	  * Computes the (absolute) difference to the successive value for each given value.
	  *
	  * @param values values
	  * @return differences to the successive value (Double.PositiveInfinity for the smallest value)
	  */
	def computeDeltaSuccessorValues(values: List[Double]): List[Double] = {
		val sortedValues = values
			.distinct
			.sorted(Ordering[Double].reverse)
		val deltaSuccessorMap = mutable.Map[Double, Double]()
		for(i <- 0 until sortedValues.length - 1) {
			val deltaSuccessor = sortedValues(i) - sortedValues(i + 1)
			deltaSuccessorMap(sortedValues(i)) = deltaSuccessor
		}
		deltaSuccessorMap(sortedValues.last) = Double.PositiveInfinity
		values.map(deltaSuccessorMap)
	}

	/**
	  * Enriches original feature values with their respective second order feature values.
	  *
	  * @param values original feature values
	  * @return original feature values and their second order feature values
	  */
	def generateMultiFeatures(values: List[Double]): List[MultiFeature] = {
		val ranks = computeRanks(values)
		val deltaTopValues = computeDeltaTopValues(values)
		val deltaSuccessorValues = computeDeltaSuccessorValues(values)
		values
			.zip(ranks)
			.zip(deltaTopValues)
			.zip(deltaSuccessorValues)
			.map { case (((value, rank), deltaTop), deltaSuccessor) =>
				MultiFeature(value, rank, deltaTop, deltaSuccessor)
			}
	}
}
