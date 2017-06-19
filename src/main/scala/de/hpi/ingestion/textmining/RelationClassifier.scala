package de.hpi.ingestion.textmining

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.deduplication.models.{PrecisionRecallDataTuple, RelationClassifierStats, SimilarityMeasureStats}
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.ClassifierTraining.averageStatistics
import de.hpi.ingestion.textmining.models.{Cooccurrence, Sentence}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

import scala.collection.mutable.ListBuffer

/**
  * Parses all Sentences with entities from Wikipedia.
  */
object RelationClassifier extends SparkJob {
	appName = "Relation Classifier"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val sentences = sc.cassandraTable[Sentence](settings("keyspace"), settings("sentenceTable"))
		val relations = sc.cassandraTable[Relation](settings("keyspace"), settings("DBpediaRelationTable"))
		val cooccurrences = sc.cassandraTable[Cooccurrence](settings("keyspace"), settings("cooccurrenceTable"))
		List(sentences).toAnyRDD() ++ List(relations).toAnyRDD() ++ List(cooccurrences).toAnyRDD()
	}

	/**
	  * Saves Sentences with entities to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.head
			.asInstanceOf[RDD[RelationClassifierStats]]
			.saveToCassandra(settings("keyspace"), settings("relClassifierStatsTable"))
	}

	// $COVERAGE-ON$

	/**
	  * Parses Bag of Words for all sentences with the given relation.
	  *
	  * @param relations tagged relations
	  * @param sentences sentences to be filtered
	  * @param rel       relation to be checked
	  * @return list of tuples of 0.0 as label and bag of words for each sentence
	  */
	def parseBagOfWordsForRelations(
		relations: RDD[Relation],
		sentences: RDD[Sentence],
		rel: String
	): RDD[(Double, List[String])] = {
		relations
			.map(rel => (List(rel.subjectentity, rel.objectentity), rel.relationtype))
			.collect {
				case (entities, relation) if relation == rel => (entities, 1.0)
			}
			.join(
				sentences
					.map(sentence => (sentence.entities.map(_.entity), sentence.bagofwords))
					.filter(_._1.length == 2)
			).values
			.map { case (relation, bagOfWords) => (relation, bagOfWords) }
	}

	/**
	  * Parses Bag of Words for all sentences with entities that have cooccurrence of 1
	  *
	  * @param cooccurrences cooccurrence in sentence
	  * @param sentences     sentences to be filtered
	  * @return list of tuples of 0.0 as label and bag of words for each sentence
	  */
	def parseBagOfWordsForNoRelations(
		cooccurrences: RDD[Cooccurrence],
		sentences: RDD[Sentence]
	): RDD[(Double, List[String])] = {
		cooccurrences
			.filter(cooc => cooc.count == 1 && cooc.entitylist.length == 2)
			.map(cooc => (cooc.entitylist, cooc.count))
			.join(
				sentences
					.map(sentence => (sentence.entities.map(_.entity), sentence.bagofwords))
					.filter(s => s._1.length == 2)
			).values
			.map { case (count, bagOfWords) => (0.0, bagOfWords) }
	}

	/**
	  * Parses Bag of Words for all sentences except the ones with the given relation.
	  *
	  * @param relations tagged relations
	  * @param sentences sentences to be filtered
	  * @param rel       relation to be checked
	  * @return list of tuples of 0.0 as label and bag of words for each sentence
	  */

	def parseBagOfWordsWithNoKnownRelations(
		relations: RDD[Relation],
		sentences: RDD[Sentence],
		rel: String
	): RDD[(Double, List[String])] = {
		relations
			.map(rel => (List(rel.subjectentity, rel.objectentity), rel.relationtype))
			.collect {
				case (entities, relation) if relation != rel => (entities, 0.0)
			}
			.join(
				sentences
					.map(sentence => (sentence.entities.map(_.entity), sentence.bagofwords))
					.filter(_._1.length == 2)
			).values
			.map { case (relation, bagOfWords) => (relation, bagOfWords) }
	}

	/**
	  * Parses Bag of Words for all sentenes expcept the ones with the given relation.
	  *
	  * @param relations tagged relations
	  * @param sentences sentences to be filtered
	  * @param rel       relation to be checked
	  * @return list of tuples of 0.0 as label and bag of words for each sentence
	  */
	def parseBagOfWordsWithNORelations(
		relations: RDD[Relation],
		sentences: RDD[Sentence],
		rel: String
	): RDD[(Double, List[String])] = {
		sentences.subtract(relations
			.map(rel => (List(rel.subjectentity, rel.objectentity), rel.relationtype))
			.collect {
				case (entities, relation) if relation == rel => (entities, rel)
			}
			.join(
				sentences
					.map(sentence => (
						sentence.entities.map(_.entity),
						(
							sentence.bagofwords,
							sentence.articleoffset,
							sentence.articletitle,
							sentence.text,
							sentence.entities
						)
					)
					)
					.filter(_._1.length == 2)
			)
			.map { case (key, (relation, (bagofwords, articleoffset, articletitle, text, entities))) =>
				Sentence(articletitle, articleoffset, text, entities, bagofwords)
			})
			.map(sentence => (0.0, sentence.bagofwords))
	}

	/**
	  * Calculates Precision, Recall and F1-Score of the given prediction results.
	  *
	  * @param predictionAndLabels RDD containing Tuples of the prediction and the given label in the format
	  *                            (prediction, label)
	  * @return List of statistic data as three-tuples in the format (statistic, threshold, value)
	  */
	def calculateStatistics(
		predictionAndLabels: RDD[(Double, Double)],
		beta: Double = 1.0
	): List[PrecisionRecallDataTuple] = {
		val metrics = new MulticlassMetrics(predictionAndLabels)
		metrics.labels.map { label =>
			val precision = metrics.precision(label)
			val recall = metrics.recall(label)
			val f1score = metrics.fMeasure(label)
			PrecisionRecallDataTuple(label, precision, recall, f1score)
		}.toList
	}

	/**
	  * Uses cross validation to learn and test a model.
	  *
	  * @param data     labeled training data
	  * @param numFolds number of folds for cross validation
	  * @return list of average statistics for model
	  */
	def crossValidate(
		data: RDD[LabeledPoint],
		numFolds: Int
	): List[PrecisionRecallDataTuple] = {
		val weights = (0 until numFolds).map(t => 1.0 / numFolds).toArray
		val folds = data.randomSplit(weights)
		val segments = folds.indices.map { index =>
			val test = folds(index)
			val training = folds.slice(0, index) ++ folds.slice(index + 1, folds.length)
			(test, training)
		}
		segments
			.map { case (test, trainingList) =>
				val training = trainingList.reduce(_.union(_))
				val model = trainNaiveBayes(training)
//				val model = trainRandomForest(training)
				val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
					val prediction = model.predict(features)
					(prediction, label)
				}
				calculateStatistics(predictionAndLabels)
			}.toList
			.flatten
	}

	/**
	  * Trains random forrest model
	  *
	  * @param trainingData labeled training data
	  * @param numTrees     number of trees used
	  * @param maxDepth     maximum depth
	  * @param maxBins      number of bins used
	  * @return random forest model
	  */
	def trainRandomForest(
		trainingData: RDD[LabeledPoint],
		numTrees: Int = 5,
		maxDepth: Int = 4,
		maxBins: Int = 32
	): RandomForestModel = {
		RandomForest.trainClassifier(
			trainingData,
			numClasses = 2,
			categoricalFeaturesInfo = Map[Int, Int](),
			numTrees = numTrees,
			featureSubsetStrategy = "auto",
			impurity = "gini",
			maxDepth = maxDepth,
			maxBins = maxDepth)
	}

	/**
	  * Trains a naive bayes model.
	  *
	  * @param trainingData labeled training data
	  * @return naive bayes model
	  */
	def trainNaiveBayes(
		trainingData: RDD[LabeledPoint]
	): NaiveBayesModel = {
		NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")
	}

	def calculateLabeledHashTFIDF(
		relationsWithBagOfWords: RDD[(Double, List[String])],
		noRelationsBagOfWords: RDD[(Double, List[String])]
	): RDD[(List[String], LabeledPoint)] = {
		val hashingTF = new HashingTF()
		val tf = (relationsWithBagOfWords ++ noRelationsBagOfWords).map { case (label, words) =>
			val tf = hashingTF.transform(words)
			(label, tf, words)
		}

		// While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
		// First to compute the IDF vector and second to scale the term frequencies by IDF.
		tf.cache()
		val idf = new IDF().fit(tf.map(_._2))
		tf.map { case (label, tf, words) =>
			val tfidf = idf.transform(tf)
			(words, LabeledPoint(label, tfidf))
		}
	}

	/**
	  * Calculates statistics for multiple relation scenarios
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val sentences = input.fromAnyRDD[Sentence]()(0)
		val relations = input.fromAnyRDD[Relation]()(1)
		val cooccurrences = input.fromAnyRDD[Cooccurrence]()(2)
		var comment = "naive Bayes, sentences except the ones with the given relation"
		val rels = Set(
			"parentCompany",
			"owningCompany",
			"successor",
			"predecessor",
			"subsidiary"
		)
		val stats = rels.map { rel =>
			val relationsWithBagOfWords = parseBagOfWordsForRelations(relations, sentences, rel)
			val noRelationsBagOfWords = parseBagOfWordsWithNoKnownRelations(relations, sentences, rel)
			//			val noRelationsBagOfWords = parseBagOfWordsForNoRelations(cooccurrences, sentences)
			//			val noRelationsBagOfWords = parseBagOfWordsWithNORelations(relations, sentences, rel)

			val labeledTfidf = calculateLabeledHashTFIDF(relationsWithBagOfWords, noRelationsBagOfWords)
			val result = crossValidate(labeledTfidf.map(_._2), 3)
			//			predictionAndLabel
			//				.filter(_._2._1 == 1.0)
			//				.collect
			//				.foreach(t => println(t._1))
			RelationClassifierStats(
				rel = rel,
				sentenceswithrelation = relationsWithBagOfWords.count.toInt,
				sentenceswithnorelation = relationsWithBagOfWords.count.toInt,
				data = result,
				comment = Option(comment))
		}.toList
		List(sc.parallelize(stats)).toAnyRDD()
	}
}