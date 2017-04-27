package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.deduplication.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.xml.XML
import scala.collection.mutable
import scala.io.Source

/**
  * Abstract class containing generic methods for the deduplication
  * @param confidence minimum confidence for two subjects to be identified as duplicates
  * @param importName name of the deduplication
  * @param dataSources sources where the new data is retrieved from
  * @param configFile name of config file in resource folder
  */
class Deduplication(
	val confidence: Double,
	val importName: String,
	val dataSources: List[String],
	val configFile: Option[String] = None,
	val merge: Boolean = false
) extends Serializable with SparkJob {
	var config = mutable.Map[String, List[ScoreConfig[String, SimilarityMeasure[String]]]]()
	val settings = mutable.Map[String, String]()
	var blockingSchemes = List[BlockingScheme](new SimpleBlockingScheme)
	appName = importName

	// $COVERAGE-OFF$
	/**
	  * Loads the Subjects and if needed the staged Subjects from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val subjects = List(sc.cassandraTable[Subject](settings("keyspaceSubjectTable"), settings("subjectTable")))
		val staging = if(merge) {
			List(sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable")))
		} else {
			Nil
		}
		(subjects ++ staging).toAnyRDD()
	}

	/**
	  * Saves the duplicates and block evaluation to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val duplicates = output.head.asInstanceOf[RDD[Subject]]
		val evaluation = output(1).asInstanceOf[RDD[BlockEvaluation]]
		duplicates.saveToCassandra(settings("keyspaceDuplicatesTable"), settings("duplicatesTable"))
		evaluation.saveToCassandra(settings("keyspaceStatsTable"), settings("statsTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Compares to subjects regarding the configuration
	  * @param subject1 subjects to be compared to subject2
	  * @param subject2 subjects to be compared to subject2
	  * @return the similarity score of the subjects
	  */
	def compare(
		subject1: Subject,
		subject2: Subject,
		scale: Int = 1
	): Double = {
		val weights = for {
			scoreConfigs <- this.config.values.toList
			scoreConfig <- scoreConfigs
			weight = scoreConfig.weight
		} yield weight

		val scores = for {
			(attribute, simMeasures) <- this.config
			values1 = subject1.get(attribute)
			values2 = subject2.get(attribute)
			if values1.nonEmpty && values2.nonEmpty
			simMeasure <- simMeasures
		} yield {
			val score = simMeasure.similarityMeasure.compare(values1.head, values2.head, simMeasure.scale)
			score * simMeasure.weight * simMeasure.scale
		}
		scores.sum / weights.sum
	}

	/**
	  * Get similarity Measure by its name.
	  * @param similarityMeasure Name of the similiarity Measure
	  * @tparam T type of the similarity Measure
	  * @return Similarity Measure
	  */
	def getSimilarityMeasure[T](similarityMeasure: String): SimilarityMeasure[T] = {
		Deduplication.dataTypes.getOrElse(similarityMeasure, ExactMatchString).asInstanceOf[SimilarityMeasure[T]]
	}

	/**
	  * Reads the configuration from a xml file
	  * @return a list containing scoreConfig entities parsed from the xml file
	  */
	def parseConfig(): Unit = {
		val path = this.configFile.getOrElse("config.xml")
		val xml = XML.loadString(Source
			.fromURL(getClass.getResource(s"/$path"))
			.getLines()
			.mkString("\n"))

		val configSettings = xml \\ "config" \ "sourceSettings"
		for(node <- configSettings.head.child if node.text.trim.nonEmpty)
			settings(node.label) = node.text

		val attributes = (xml \\ "config" \ "simMeasurements" \ "attribute").toList
		val similarityMeasure = for {
			node <- attributes
			key = (node \ "key").text
			feature <- (node \ "feature").toList
			similarityMeasure = (feature \ "similarityMeasure").text
			weight = (feature \ "weight").text.toDouble
			scale = (feature \ "scale").text.toInt
		}{
			val scoreConfig = ScoreConfig[String, SimilarityMeasure[String]](
				key,
				getSimilarityMeasure[String](similarityMeasure),
				weight,
				scale
			)
			this.config(key) = config.getOrElse(key, Nil):::List(scoreConfig)
		}
	}

	/**
	  * Uses input blocking scheme to generate blocks of subjects.
	  * @param subjects RDD to perform the blocking on.
	  * @param blockingSchemes blocking schemes to use to generate keys.
	  * @return subjects grouped by blocking keys
	  */
	def blocking(
		subjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme]
	): RDD[(List[List[String]], List[Subject])] = {
		subjects
			.map(subject => (blockingSchemes.map(_.generateKey(subject)), List(subject)))
			.reduceByKey(_ ++ _)	// ToDo: #182, better reduce function
	}

	/**
	  * Creates blocks based on the input blocking scheme
	  * @param subjects RDD to perform the blocking on
	  * @param stagingSubjects RDD to perform the blocking on
	  * @param blockingSchemes blocking schemes to use to generate keys.
	  * @return RDD of blocks.
	  */
	def blocking(
		subjects: RDD[Subject],
		stagingSubjects: RDD[Subject],
		blockingSchemes: List[BlockingScheme]
	): RDD[(List[List[String]], List[Subject])] = {
		val subjectBlocks = blocking(subjects, blockingSchemes)
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes)
		subjectBlocks
			.fullOuterJoin(stagingBlocks)
			.map {
				case (key, (Some(x), None)) => (key, x)
				case (key, (None, Some(x))) => (key, x)
				case (key, (Some(x), Some(y))) => (key, (x ++ y).distinct)
			}
	}

	/**
	  * Creates candidates for the Deduplication for each element from the output of findDuplicates
	  * @param subjectPairs Pairs of subjects for all possible duplicates
	  * @return all actual candidates to save to the cassandra
	  */
	def createDuplicateCandidates(subjectPairs: RDD[(Subject, Subject, Double)]): RDD[DuplicateCandidates] = {
		subjectPairs
			.map { case (subject1, subject2, score) =>
				subject1.id -> List((subject2, settings("stagingTable"), score))
			}
			.reduceByKey { case (accumulator, candidate) => candidate ::: accumulator}
			.map(DuplicateCandidates.tupled)
	}

	/**
	  * Finds the duplicates of each block
	  * @param block List of Subjects where the duplicates are searched in
	  * @return tuple of Subjects with it's score, which is greater or equal a given threshold this.confidence
	  */
	def findDuplicates(block: List[Subject]): List[(Subject, Subject, Double)] = {
		block
			.cross(block)
			.filter(tuple => tuple._1 != tuple._2)
			.map(tuple => Set(tuple, tuple.swap))
			.toList
			.distinct
			.map(_.head)
			.map(tuple => (tuple._1, tuple._2, compare(tuple._1, tuple._2)))
			.filter(tuple => tuple._3 >= this.confidence)
	}

	/**
	  * Add symmetric relations between two subject nodes, abstraction from SubjectManagers
	  * addRelations method
	  * @param subject1 UUID of a given node
	  * @param subject2 UUID of another given node
	  * @param relations A set of attributes of keys and values describing the relation,
	  * typically a 'type' value should be passed
	  * @param version Current transactional id handle
	  */
	def addSymRelation(
		subject1: Subject,
		subject2: Subject,
		relations: Map[String,String],
		version: Version
	): Unit = {
		val subject_manager1 = new SubjectManager(subject1, version)
		val subject_manager2 = new SubjectManager(subject2, version)

		subject_manager1.addRelations(Map(subject2.id -> relations))
		subject_manager2.addRelations(Map(subject1.id -> relations))
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
				Map("type" -> "isDuplicate", "confidence" -> (scoredDuplicatePair._3).toString),
				version
			)
		}
	}

	/**
	  * Executes methods on the blocked data to gain information.
	  *
	  * @param blocks  Input data, partitioned in blocks.
	  * @param comment Note to add to the evaluated data.
	  * @return Blocks with size-value and comment.
	  */
	def evaluateBlocks(
		blocks: RDD[(List[List[String]], List[Subject])],
		comment: String
	): BlockEvaluation = {
		val dataMap = blocks
			.map(block => (block._1.map(_.mkString(" ")), block._2.size))
			.sortBy(_._2, false)
			.collect()
			.toMap
		BlockEvaluation(data = dataMap, comment = Option(comment))
	}

	/**
	  * Blocks the Subjects and finds duplicates between them between the Subjects and staged Subjects.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		parseConfig()
		val inputData = input.fromAnyRDD[Subject]()
		val subjects = inputData.head
		val blocks = if(merge) {
			val staging = inputData(1)
			blocking(subjects, staging, blockingSchemes)
		} else {
			blocking(subjects, blockingSchemes)
		}
		val blockEvaluation = sc.parallelize(Seq(evaluateBlocks(blocks, appName)))

		val subjectPairs = blocks.flatMap(block => findDuplicates(block._2))
		val duplicates = createDuplicateCandidates(subjectPairs)

		List(duplicates).toAnyRDD() ++ List(blockEvaluation).toAnyRDD()
	}
}

object Deduplication {
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
}
