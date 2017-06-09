package de.hpi.ingestion.deduplication

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.mllib.classification.NaiveBayesModel

class ClassificationTrainingTest extends FlatSpec with Matchers with SharedSparkContext {

	"Naive Bayes model" should "be returned" in {
		val entries = (0 to 10).flatMap(t => TestData.featureEntries)
		val input = List(sc.parallelize(entries)).toAnyRDD()
		val models = ClassificationTraining.run(input, sc).head.collect
		models should not be empty
		models.foreach { model =>
			model.isInstanceOf[NaiveBayesModel] shouldBe true }
	}
}
