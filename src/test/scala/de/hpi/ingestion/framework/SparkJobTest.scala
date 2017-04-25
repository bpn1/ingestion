package de.hpi.ingestion.framework

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.framework.mock.MockSparkJob
import org.scalatest.{FlatSpec, Matchers}

class SparkJobTest extends FlatSpec with Matchers with SharedSparkContext {

	"Main method" should "call the methods in the proper sequence" in {
		val sparkJob = new MockSparkJob
		sparkJob.main(Array[String]())
		val expectedSequence = List("assertConditions", "sparkContext", "load", "run", "save")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}

	"Assert conditions" should "be able to stop the job" in {
		val sparkJob = new MockSparkJob
		sparkJob.main(Array[String]("test"))
		val expectedSequence = List("assertConditions")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}
}
