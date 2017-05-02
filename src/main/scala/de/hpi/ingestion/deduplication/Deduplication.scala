package de.hpi.ingestion.deduplication

import java.util.UUID

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.implicits.TupleImplicits._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.framework.SparkJob
import scala.language.postfixOps
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
	val configFile: Option[String] = None
) extends Serializable with SparkJob {
	var config = List[ScoreConfig[String, SimilarityMeasure[String]]]()
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
		val staging = List(sc.cassandraTable[Subject](settings("keyspaceStagingTable"), settings("stagingTable")))
		(subjects ++ staging).toAnyRDD()
	}

	/**
	  * Saves the duplicates and block evaluation to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val duplicates = output.head.asInstanceOf[RDD[DuplicateCandidates]]
		val evaluation = output(1).asInstanceOf[RDD[(UUID, Option[String], Map[String, Int])]]
		duplicates.saveToCassandra(settings("keyspaceDuplicatesTable"), settings("duplicatesTable"))
		evaluation
			.saveToCassandra(
				settings("keyspaceStatsTable"),
				settings("statsTable"),
				SomeColumns("id", "comment", "data" append))
	}
	// $COVERAGE-ON$

	/**
	  * Parses config before calling the implemented method in the SparkJob trait.
	  * @param args arguments of the program
	  * @return true if the program can continue, false if it should be terminated
	  */
	override def assertConditions(args: Array[String]): Boolean = {
		parseConfig()
		super.assertConditions(args)
	}

	/**
	  * Compares to subjects regarding the configuration
	  * @param subject1 subjects to be compared to subject2
	  * @param subject2 subjects to be compared to subject1
	  * @return the similarity score of the subjects
	  */
	def compare(
		subject1: Subject,
		subject2: Subject,
		scale: Int = 1
	): Double = {
		val scoreList = this.config
			.map { feature =>
				val attribute = feature.key
				val valueSubject1 = subject1.get(attribute)
				val valueSubject2 = subject2.get(attribute)
				(feature, valueSubject1, valueSubject2)
			}.collect {
				case (feature, values1, values2) if values1.nonEmpty && values2.nonEmpty =>
					CompareStrategy(feature.key)(values1, values2, feature)
			}
		scoreList.sum / Math.max(1, scoreList.length)
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

		config = (xml \\ "config" \ "simMeasurements" \ "feature")
			.toList
			.map { feature =>
				val attribute = (feature \ "attribute").text
				val simMeasure = (feature \ "similarityMeasure").text
				val weight = (feature \ "weight").text
				val scale = (feature \ "scale").text

				ScoreConfig[String, SimilarityMeasure[String]](
					attribute,
					Deduplication.dataTypes
						.getOrElse(simMeasure, ExactMatchString)
						.asInstanceOf[SimilarityMeasure[String]],
					weight.toDouble,
					scale.toInt)
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
	): List[RDD[(String, List[Subject])]] = {
		blockingSchemes.map { scheme =>
			subjects.flatMap { subject =>
				val blockingKeys = scheme.generateKey(subject).distinct
				blockingKeys.map((_, List(subject)))
			}.filter(_._1 != scheme.undefinedValue)
			.reduceByKey(_ ::: _)
		}
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
	): List[RDD[(String, List[Subject])]] = {
		val subjectBlocks = blocking(subjects, blockingSchemes)
		val stagingBlocks = blocking(stagingSubjects, blockingSchemes)

		(subjectBlocks, stagingBlocks).zipped.map { case (subjectBlock, stagingBlock) =>
			subjectBlock
				.fullOuterJoin(stagingBlock)
				.mapValues { case (subjectList, stagingList) =>
					val subjects = subjectList.getOrElse(Nil)
					val stagedSubjects = stagingList.getOrElse(Nil)
					subjects.union(stagedSubjects).distinct
				}
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
				(subject1.id, List((subject2, settings("stagingTable"), score)))
			}.reduceByKey(_ ::: _)
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
				Map("type" -> "isDuplicate", "confidence" -> scoredDuplicatePair._3.toString),
				version
			)
		}
	}

	/**
	  * Executes methods on the blocked data to gain information.
	  *
	  * @param blockingList Input data, partitioned in blocks.
	  * @param comment Note to add to the evaluated data.
	  * @return Blocks with size-value and comment.
	  */
	def evaluateBlocks(
		blockingList: List[RDD[(String, List[Subject])]],
		comment: String,
		blockingSchemes: List[BlockingScheme]
	): RDD[(UUID, Option[String], Map[String, Int])] = {
		(blockingList, blockingSchemes).zipped.map { case (blocks, blockingScheme) =>
			val id = UUID.randomUUID()
			val blockComment = Option(s"${comment}_${blockingScheme.tag}")
			blocks
				.map(block => List(block.map(identity, _.length)))
				.map(data => (id, blockComment, data.toMap))
		}.reduce(_.union(_))
	}

	/**
	  * Blocks the Subjects and finds duplicates between them between the Subjects and staged Subjects.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val inputData = input.fromAnyRDD[Subject]()
		val subjects = inputData.head
		val staging = inputData(1)
		val blocks = blocking(subjects, staging, blockingSchemes)
		val blockEvaluation = evaluateBlocks(blocks, s"${this.appName}_${System.currentTimeMillis()}", blockingSchemes)
		val evaluationOutput = List(blockEvaluation).toAnyRDD()

		val subjectPairs = blocks.map { blocking =>
			blocking.flatMap(block => findDuplicates(block._2))
		}.reduce(_.union(_))
		val duplicates = createDuplicateCandidates(subjectPairs)
		List(duplicates).toAnyRDD() ++ evaluationOutput
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
