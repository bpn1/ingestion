package de.hpi.ingestion.textmining

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import de.hpi.ingestion.implicits.CollectionImplicits._

class TfidfProfilerTest extends FlatSpec with SharedSparkContext with Matchers {
	"Runtime statistics" should "not be empty" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val input = List(articles).toAnyRDD()
		val List(sparkTfidfRDD, ingestionTfidfRDD, runtimesRDD) = TfidfProfiler.run(input, sc)
		val runtimes = runtimesRDD
			.asInstanceOf[RDD[(String, Long)]]
			.collect
		runtimes should not be empty
	}

	they should "contain exactly these labels" in {
		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val input = List(articles).toAnyRDD()
		val List(sparkTfidfRDD, ingestionTfidfRDD, runtimesRDD) = TfidfProfiler.run(input, sc)
		val statisticLabels = runtimesRDD
			.asInstanceOf[RDD[(String, Long)]]
			.map(_._1)
			.collect
			.toSet
		statisticLabels shouldEqual TestData.profilingStatisticLables()
	}

	"Calculated tf-idf values" should "not be empty" in {
		val oldDocFreqStreamFunction = CosineContextComparator.docFreqStreamFunction
		val testDocFreqFunction = TestData.docfreqStream("smalldocfreq") _
		CosineContextComparator.docFreqStreamFunction = testDocFreqFunction

		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val input = List(articles).toAnyRDD()
		val List(sparkTfidfRDD, ingestionTfidfRDD, runtimesRDD) = TfidfProfiler.run(input, sc)
		val sparkTfidf = sparkTfidfRDD
			.asInstanceOf[RDD[Vector]]
			.collect
		val ingestionTfidf = ingestionTfidfRDD
			.asInstanceOf[RDD[(String, Map[String, Double])]]
			.collect
			.toSet

		sparkTfidf should not be empty
		sparkTfidf.foreach(_.toArray should not be empty)
		ingestionTfidf should not be empty
		ingestionTfidf.foreach(_._2 should not be empty)

		CosineContextComparator.docFreqStreamFunction = oldDocFreqStreamFunction
	}

	they should "be exactly these tf-idf values" in {
		val oldDocFreqStreamFunction = CosineContextComparator.docFreqStreamFunction
		val testDocFreqFunction = TestData.docfreqStream("smalldocfreq") _
		CosineContextComparator.docFreqStreamFunction = testDocFreqFunction

		val articles = sc.parallelize(TestData.parsedWikipediaWithTextsSet().toList)
		val input = List(articles).toAnyRDD()
		val List(sparkTfidfRDD, ingestionTfidfRDD, runtimesRDD) = TfidfProfiler.run(input, sc)
		val sparkTfidf = sparkTfidfRDD
			.asInstanceOf[RDD[Vector]]
			.collect
		val ingestionTfidf = ingestionTfidfRDD
			.asInstanceOf[RDD[(String, Map[String, Double])]]
			.collect
			.toSet

		sparkTfidf shouldEqual TestData.profilerTfidfVectors()
		ingestionTfidf shouldEqual TestData.profilerTfidfMaps()

		CosineContextComparator.docFreqStreamFunction = oldDocFreqStreamFunction
	}
}
