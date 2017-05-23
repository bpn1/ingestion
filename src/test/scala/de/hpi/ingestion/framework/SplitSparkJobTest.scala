package de.hpi.ingestion.framework

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.framework.mock.MockSplitSparkJob
import org.scalatest.{FlatSpec, Matchers}

class SplitSparkJobTest extends FlatSpec with Matchers with SharedSparkContext {

	"Main method" should "call the methods in the proper sequence" in {
		val sparkJob = new MockSplitSparkJob
		sparkJob.main(Array[String]())
		val expectedSequence = List("assertConditions", "sparkContext", "execQ", "load", "execQ", "split",
			"run", "save", "run", "save", "run", "save")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}
}
