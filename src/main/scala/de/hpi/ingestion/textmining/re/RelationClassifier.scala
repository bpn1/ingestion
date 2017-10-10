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

package de.hpi.ingestion.textmining.re

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.deduplication.models.PrecisionRecallDataTuple
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.ClassifierTraining._
import de.hpi.ingestion.textmining.models.{Cooccurrence, RelationClassifierStats, Sentence}
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkContext, sql}

/**
  * Trains a classifier on the relations from DBpedia.
  */
class RelationClassifier extends SparkJob {
	import RelationClassifier._
	appName = "Relation Classifier"
	configFile = "textmining.xml"

	var sentences: RDD[Sentence] = _
	var relations: RDD[Relation] = _
	var cooccurrences: RDD[Cooccurrence] = _
	var classifierStatistics: RDD[RelationClassifierStats] = _

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		sentences = sc.cassandraTable[Sentence](settings("keyspace"), settings("sentenceTable"))
		relations = sc.cassandraTable[Relation](settings("keyspace"), settings("DBpediaRelationTable"))
		cooccurrences = sc.cassandraTable[Cooccurrence](settings("keyspace"), settings("cooccurrenceTable"))
	}

	/**
	  * Saves Sentences with entities to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		classifierStatistics.saveToCassandra(settings("keyspace"), settings("relClassifierStatsTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Calculates statistics for multiple relation scenarios
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val filteredSentences = sentences
			.filter(sentence => sentence.entities.map(_.entity).toSet.size > 1 && sentence.entities.size == 2)
		val filteredRelations = relations.filter(relation => relation.subjectentity != relation.objectentity)
		val groupedCooccurrences = cooccurrences
			.map(cooc => (cooc.entitylist.toSet, cooc.count))
			.reduceByKey(_ + _)
		val comment = "Linear Regression, aggregated, undirected, stemmed, 10 folds, minCooc 1"
		val rels = Set(
			Set("parentCompany", "owningCompany", "subsidiary", "division")
		)
		val stats = rels.map { rel =>
			val minCooc = groupedCooccurrences.filter(_._2 <= 1).map(_._1)
			val relationsWithBagOfWords = parseBagOfWordsWithRelations(
				filteredRelations,
				filteredSentences,
				minCooc,
				rel)
			val noRelationsBagOfWords = parseBagOfWordsWithNoRelations(
				filteredRelations,
				filteredSentences,
				minCooc,
				rel)
			val session = SparkSession.builder().getOrCreate()
			import session.implicits._
			val labeledTfidf = calculateLabeledHashTFIDF(
				(relationsWithBagOfWords ++ noRelationsBagOfWords).toDF("label", "words")
			)
			val data = crossValidateWithWeights(labeledTfidf, 10)
			RelationClassifierStats(
				rel = "owns",
				sentenceswithrelation = relationsWithBagOfWords.count.toInt,
				sentenceswithnorelation = noRelationsBagOfWords.count.toInt,
				average = averageStatistics(data),
				data = data,
				comment = Option(comment)
			)
		}.toList
		classifierStatistics = sc.parallelize(stats)
	}
}

object RelationClassifier {
	val aggregated = true

	/**
	  * Parses Bag of Words for all sentences with the given relation.
	  *
	  * @param relations tagged relations
	  * @param sentences sentences to be filtered
	  * @param rel       relation to be checked
	  * @return list of tuples of 0.0 as label and bag of words for each sentence
	  */
	def parseBagOfWordsWithRelations(
		relations: RDD[Relation],
		sentences: RDD[Sentence],
		cooccurrences: RDD[Set[String]],
		rel: Set[String]
	): RDD[(Double, List[String])] = {
		val blacklist = cooccurrences.collect.toSet
		var entitiesWithBagOfWords = relations
			.map(rel => (Set(rel.subjectentity, rel.objectentity), rel.relationtype))
			.collect {
				case (entities, relation) if rel.contains(relation) => (entities, 1.0)
			}.distinct
			.join(
				sentences
					.filter(sentence => !blacklist.contains(sentence.entities.map(_.entity).toSet))
					.map(sentence => Tuple2(sentence.entities.map(_.entity).toSet, sentence.bagofwords))
			).map { case (entities, (label, bagOfWords)) => (entities, bagOfWords) }
		if(aggregated) entitiesWithBagOfWords = entitiesWithBagOfWords.reduceByKey(_ ++ _)
		entitiesWithBagOfWords.map { case (entities, bagsOfWords) => (1.0, bagsOfWords) }
	}

	/**
	  * Parses Bag of Words for all sentenes expcept the ones with the given relation.
	  *
	  * @param relations tagged relations
	  * @param sentences sentences to be filtered
	  * @param rel       relation to be checked
	  * @return list of tuples of 0.0 as label and bag of words for each sentence
	  */
	def parseBagOfWordsWithNoRelations(
		relations: RDD[Relation],
		sentences: RDD[Sentence],
		cooccurrences: RDD[Set[String]],
		rel: Set[String]
	): RDD[(Double, List[String])] = {
		val blacklist = cooccurrences.collect.toSet
		var entitiesWithBagOfWords = sentences.subtract(
			relations
				.map(rel => (Set(rel.subjectentity, rel.objectentity), rel.relationtype))
				.collect { case (entities, relation) if rel.contains(relation) => (entities, 0.0) }
				.distinct
				.join(sentences.map(sentence => Tuple2(sentence.entities.map(_.entity).toSet, sentence)))
				.map { case (key, (relation, sentence)) => sentence }
		)
			.filter(sentence => !blacklist.contains(sentence.entities.map(_.entity).toSet))
			.map(sentence => (sentence.entities.map(_.entity), sentence.bagofwords))
		if(aggregated) entitiesWithBagOfWords = entitiesWithBagOfWords.reduceByKey(_ ++ _)
		entitiesWithBagOfWords.map { case (entities, bagsOfWords) => (0.0, bagsOfWords) }
	}

	/**
	  * Uses cross validation to learn and test a model.
	  *
	  * @param data     labeled training data
	  * @param numFolds number of folds for cross validation
	  * @return list of average statistics for model
	  */
	def crossValidateWithWeights(
		data: sql.DataFrame,
		numFolds: Int
	): List[PrecisionRecallDataTuple] = {
		val numNegatives = data.filter(data("label") === 0).count
		val datasetSize = data.count
		val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize
		val calculateWeights = udf { d: Double =>
			if(d == 0.0) {
				1 * balancingRatio
			}
			else {
				1 * (1.0 - balancingRatio)
			}
		}
		val weightedDataset = data.withColumn("classWeightCol", calculateWeights(data("label")))
		val weights = (0 until numFolds).map(t => 1.0 / numFolds).toArray
		val folds = weightedDataset.randomSplit(weights)
		val segments = folds.indices.map { index =>
			val test = folds(index)
			val training = folds.slice(0, index) ++ folds.slice(index + 1, folds.length)
			(test, training)
		}
		segments
			.map { case (test, trainingList) =>
				val training = trainingList.reduce(_.union(_))
				val model = weightedLogisticRegressionDFModel()
				val trained = trainWeightedLogisticRegressionDF(training, model)
				val predictions = trained.transform(test)
				val entitiesPredictionAndLabels = predictions
					.select("prediction", "indexedLabel")
					.rdd
					.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
				calculateStatistics(entitiesPredictionAndLabels)
			}.toList
	}

	/**
	  * Calculates the tf-idf of the bag of words using the Spark-ML lib hashing tf-idf algorithm.
	  *
	  * @param relationsWithBagOfWords Data Frame containing the relations and their bag of words
	  * @return Data Frame containing the relatins and the tf-idf vectors of their bag of words
	  */
	def calculateLabeledHashTFIDF(
		relationsWithBagOfWords: sql.DataFrame
	): sql.DataFrame = {
		val hashingTF = new HashingTF()
			.setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000)
		val featurizedData = hashingTF.transform(relationsWithBagOfWords)
		// While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
		// First to compute the IDF vector and second to scale the term frequencies by IDF.
		val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
		val idfModel = idf.fit(featurizedData)

		idfModel.transform(featurizedData)
	}
}
