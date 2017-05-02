
import scala.collection.mutable
import scala.io.Source
import scala.xml.XML


object DeduplicationJob extends SparkJob {
	appName = "Deduplication"
	var config = List[ScoreConfig[String, SimilarityMeasure[String]]]()
	val settings = mutable.Map[String, String]()
	val configFile: Option[String] = None

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

	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {


		subjectManager1.addRelations(Map(subject2.id -> relations))
		subjectManager2.addRelations(Map(subject1.id -> relations))
	}

	/**
	  * Merging two duplicates in one subject
	  * @param duplicates tuple of Subject
	  * @return subjects containing the merged information of both duplicates
	  */
	def buildDuplicatesSCC(duplicates: List[(Subject, Subject, Double)], version: Version): Unit = {

		duplicates.foreach { scoredDuplicatePair =>
			addSymRelation(
				scoredDuplicatePair._1,
				scoredDuplicatePair._2,
				Map("type" -> "isDuplicate", "confidence" -> scoredDuplicatePair._3.toString),
				version
			)
		}
	}
}
