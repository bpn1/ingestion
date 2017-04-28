package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.deduplication.BlockingJob.{getClass, settings}
import de.hpi.ingestion.deduplication.models.ScoreConfig
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.io.Source
import scala.xml.XML


object DeduplicationJob extends SparkJob {
	appName = "Deduplication"
	var config = List[ScoreConfig[String, SimilarityMeasure[String]]]()
	val settings = mutable.Map[String, String]()
	var blockingSchemes = List[BlockingScheme](new SimpleBlockingScheme)

	val dataTypes: Map[String, SimilarityMeasure[_]] = Map(
		"ExactMatchString" -> ExactMatchString,
		"MongeElkan" -> MongeElkan,
		"Jaccard" -> Jaccard,
		"DiceSorensen" -> DiceSorensen,
		"Jaro" -> Jaro,
		"JaroWinkler" -> JaroWinkler,
		"N-Gram" -> NGram,
		"Overlap" -> Overlap,
		"EuclidianDistance" -> EuclidianDistance,
		"RoughlyEqualNumbers" -> RoughlyEqualNumbers
	)

	// $COVERAGE-OFF$

	/**
	  * Loads the Subjects and if needed the staged Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = List(sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable")))
		val staging = List(sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable")))
		(subjects ++ staging).toAnyRDD()
	}

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {

	}
	// $COVERAGE-ON$

	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String]): List[RDD[Any]] = {

	}

	/**
	  * Reads the configuration from a xml file
	  * @return a list containing scoreConfig entities parsed from the xml file
	  */
	def parseConfig(): Unit = {
		val path = this.configFile.getOrElse("config.xml")
		val xml = XML.loadString(
			Source
			.fromURL(getClass.getResource(s"/$path"))
			.getLines()
			.mkString("\n")
		)

		val configSettings = xml \\ "config" \ "sourceSettings"
		for(node <- configSettings.head.child if node.text.trim.nonEmpty)
			settings(node.label) = node.text
	}
}



/*object DeduplicationJob {
	def main(args: Array[String]): Unit = {
		val deduplication = new Deduplication(0.5, "TestDeduplication", List("testSource"))

		deduplication.blockingSchemes = List(
			geoCountryBlockingScheme,
			generalSectorBlockingScheme,
			simpleBlockingScheme
		)
		deduplication.main(args)
	}

}*/
