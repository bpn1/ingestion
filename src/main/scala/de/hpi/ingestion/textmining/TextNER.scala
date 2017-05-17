package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.{OffsetToken, ParsedWikipediaEntry}
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
	  * @return
	  */
	def generateDataPoints(text: String, tokenizer: IngestionTokenizer): List[(OffsetToken, LabeledPoint)] = {
		List((OffsetToken("lorem", 0, 5), LabeledPoint(0.0, new DenseVector(Array(0, 0, 0)))))
		//		val tokens = tokenizer.processWithOffsets(text)
	}

	/**
	  * todo
	  *
	  * @param data
	  * @return
	  */
	def findNamedEntities(data: List[(OffsetToken, LabeledPoint)], classifier: RandomForestModel): List[OffsetToken] = {
		val predictionsAndLabels = data.map { case (offsetToken, LabeledPoint(label, features)) =>
			val prediction = classifier.predict(features)
			(prediction, label)
		}

		List(OffsetToken("lorem", 0, 5))
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
		val tokenizer = IngestionTokenizer(true, true)
		val namedEntities = textsRDD.map { text =>
			val data = generateDataPoints(text, tokenizer)
			findNamedEntities(data, classifier)
		}
		List(namedEntities).toAnyRDD()
	}
}
