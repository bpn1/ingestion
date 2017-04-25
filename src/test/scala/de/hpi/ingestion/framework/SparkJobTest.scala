package de.hpi.ingestion.framework

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.framework.mock.{MockConditionSparkJob, MockSparkJob}
import org.scalatest.{FlatSpec, Matchers}

class SparkJobTest extends FlatSpec with Matchers with SharedSparkContext {

	"Main method" should "call the methods in the proper sequence" in {
		val sparkJob = new MockSparkJob
		sparkJob.main(Array[String]())
		val expectedSequence = List("assertConditions", "sparkContext", "load", "run", "save")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}

	"Assert conditions" should "not stop the job if the conditions are true" in {
		val sparkJob = new MockConditionSparkJob
		sparkJob.main(Array[String]())
		val expectedSequence = List("assertConditions", "sparkContext", "load", "run", "save")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}

	it should "stop the job if the conditions are false" in {
		val sparkJob = new MockConditionSparkJob
		sparkJob.main(Array[String]("test"))
		val expectedSequence = List("assertConditions")
		sparkJob.methodCalls.toList shouldEqual expectedSequence
	}

	"Spark Conf options" should "be set to the correct values" in {
		val sparkJob = new MockSparkJob
		val testName = "Mock Spark Job"
		val testOptions = Map("option1" -> "value1", "option2" -> "value2")
		sparkJob.appName = testName
		sparkJob.sparkOptions ++= testOptions
		val conf = sparkJob.createSparkConf()
		conf.get("spark.app.name") shouldEqual testName
		conf.get("option1") shouldEqual testOptions("option1")
		conf.get("option2") shouldEqual testOptions("option2")
	}
}
