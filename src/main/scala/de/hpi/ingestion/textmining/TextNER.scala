package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{LinkCandidate, OffsetToken, ParsedWikipediaEntry}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel

/**
  * todo
  */
object TextNER extends SparkJob {
	appName = "Text NER"
	val keyspace = "wikidumps"
	val tablename = "parsedwikipedia"
	var classifierModel = "wikipedia_randomforest_model_1494495223381"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikipedia = sc.cassandraTable[ParsedWikipediaEntry](keyspace, tablename)
		List(wikipedia).toAnyRDD()
	}

	/**
	  * todo
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val timestamp = System.currentTimeMillis / 1000
		output
			.fromAnyRDD[OffsetToken]()
			.head
			.saveAsTextFile(s"named_entities_$timestamp")
	}

	// $COVERAGE-ON$

	/**
	  * todo
	  *
	  * @param text
	  * @param tokenizer
	  * @return
	  */
	def generateLinkCandiates(text: String, tokenizer: IngestionTokenizer): List[LinkCandidate] = {
		tokenizer.processWithOffsets(text)
			.map(offsetToken => LinkCandidate(offsetToken, "Audi"))
	}

	/**
	  * todo
	  *
	  * @return
	  */
	def generateDataPoints(linkCandidates: List[LinkCandidate]): List[(LinkCandidate, LabeledPoint)] = {
		linkCandidates.map { linkCandidate =>
			(linkCandidate, LabeledPoint(0.0, new DenseVector(Array(0, 0, 0))))
		}
	}

	/**
	  * todo
	  *
	  * @param data
	  * @return
	  */
	def findNamedEntities(
		data: List[(LinkCandidate, LabeledPoint)],
		classifier: RandomForestModel
	): List[LinkCandidate] = {
		val predictionsAndLabels = data.map { case (linkCandidate, LabeledPoint(label, features)) =>
			val prediction = classifier.predict(features)
			(prediction, label)
		}

		List(LinkCandidate(OffsetToken("lorem", 0, 5), "ipsum"))
	}

	/**
	  * todo
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val texts = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
			.take(100)
			.map(_.getText())
		val textsRDD = sc.parallelize(texts)
		val classifier = RandomForestModel.load(sc, classifierModel)
		val tokenizer = IngestionTokenizer(true, false)
		val namedEntities = textsRDD.map { text =>
			val linkCanditates = generateLinkCandiates(text, tokenizer)
			val data = generateDataPoints(linkCanditates)
			findNamedEntities(data, classifier)
		}
		List(namedEntities).toAnyRDD()
	}
}
