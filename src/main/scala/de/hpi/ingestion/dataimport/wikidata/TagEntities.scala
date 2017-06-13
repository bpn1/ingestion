package de.hpi.ingestion.dataimport.wikidata

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd._
import scala.language.postfixOps
import de.hpi.ingestion.dataimport.wikidata.models.{SubclassEntry, WikiDataEntity}
import de.hpi.ingestion.framework.SparkJob

/**
  * This job builds the subclass hierarchy of selected Wikidata classes and tags every Wikidata entity, that is an
  * instance of one of the subclasses, with the top level class.
  */
object TagEntities extends SparkJob {
	appName = "TagEntities"
	configFile = "wikidata_import.xml"
	val instanceProperty = "instance of"
	val subclassProperty = "subclass of"
	val tagClasses = Map(
		"Q268592" -> "economic branch",
		"Q362482" -> "operation",
		"Q179076" -> "exchange",
		"Q650241" -> "financial institutions",
		"Q1664720" -> "institute",
		"Q563787" -> "health maintenance organization",
		"Q79913" -> "non-governmental organization",
		"Q783794" -> "company",
		"Q7275" -> "state",
		"Q1785733" -> "environmental organization",
		"Q15911314" -> "association",
		"Q6881511" -> "enterprise",
		"Q4287745" -> "medical organization",
		"Q2222986" -> "lobbying organization",
		"Q672386" -> "collection agency",
		"Q1331793" -> "media company",
		"Q1123526" -> "chamber of commerce",
		"Q1047437" -> "copyright collective",
		"Q4830453" -> "business enterprise"
	)

	// $COVERAGE-OFF$
	/**
	  * Reads the Wikidata entities from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = sc.cassandraTable[WikiDataEntity](settings("keyspace"), settings("wikidataTable"))
		List(wikidata).toAnyRDD()
	}

	/**
	  * Saves the tagged Wikidata entities to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[(String, String, Map[String, List[String]])]()
			.head
			.saveToCassandra(
				settings("keyspace"),
				settings("wikidataTable"),
				SomeColumns("id", "instancetype", "data" append))
	}
	// $COVERAGE-ON$

	/**
	  * Adds entries of new class path map to the old map if they are new or shorter than the
	  * current path.
	  * @param subclasses Map of new subclass path entries
	  * @param oldClasses Map of current subclass path entries
	  * @return Map of merged subclass path entries containing all new entries and shorter paths
	  */
	def addShorterPaths(
		subclasses: Map[String, List[String]],
		oldClasses: Map[String, List[String]]
	): Map[String, List[String]] = {
		subclasses ++ oldClasses.filterNot { case (key, path) =>
			subclasses.get(key).exists(_.size <= path.size)
		}
	}

	/**
	  * Builds a map of subclass paths given a list of subclass entries containing
	  * the {@subclassProperty} property.
	  * @param categoryData RDD of Subclass Entries containing the subclass information.
	  * @param searchClasses Map of superclasses with id and name for which the subclass
	  * information is gathered.
	  * @return Map of subclass id pointing to the path of superclasses
	  */
	def buildSubclassMap(
		categoryData: RDD[SubclassEntry],
		searchClasses: Map[String, String]
	): Map[String, List[String]] = {
		searchClasses.map { case (wikiDataId, tag) =>
			var oldClasses = Map(wikiDataId -> List(tag))
			var subclasses = Map[String, List[String]]()

			var addedElements = true
			while(addedElements) {
				subclasses ++= addShorterPaths(subclasses, oldClasses)

				// iteratively append next layer of subclass tree
				val newClasses = categoryData
					.map(entry => entry.copy(classList = entry.data(subclassProperty).filter(oldClasses.contains)))
					.flatMap { case SubclassEntry(id, label, data, classList) =>
						classList.headOption
							.flatMap(oldClasses.get)
							.map(newClassList => (id, newClassList :+ label))
					}.collect
					.toMap

				// check if new elements were added and not stuck in a loop
				addedElements = newClasses.nonEmpty && newClasses.keySet != oldClasses.keySet
				oldClasses = newClasses
			}
			subclasses
		}.reduce(_ ++ _)
	}

	/**
	  * Translates a Wikidata entity into a Subclass entry. This entry contains the id, the label
	  * or the id if there is no label and the data entries with either the {@instanceProperty} or
	  * {@subclassProperty} key.
	  * @param entity Wikidata entity to translate
	  * @return SubclassEntry with only the id, label and subclass or instance property
	  */
	def translateToSubclassEntry(entity: WikiDataEntity): SubclassEntry = {
		val data = entity.data.filterKeys(key =>
			key == instanceProperty || key == subclassProperty)
		val label = entity.label.getOrElse(entity.id)
		SubclassEntry(entity.id, label, data)
	}

	/**
	  * Creates value of the instancetype column and creates new data entry containing the subclass
	  * path for a Wikidata entity.
	  * @param entry Subclass entry containing the id and data of the Wikidata entry
	  * @param classes map of class data used to set subclass path and set instancetype
	  * @return triple of wikidata id, instancetype and path property
	  */
	def updateInstanceOfProperty(
		entry: SubclassEntry,
		classes: Map[String, List[String]]
	): (String, String, Map[String, List[String]]) = {
		val classKey = entry.data(instanceProperty)
			.filter(classes.contains)
			.head
		val path = classes(classKey)
		(entry.id, path.head, Map(settings("wikidataPathProperty") -> path))
	}

	/**
	  * Updates the instancetype and wikidata path for all entries which are an instance of a class listed in the
	  * subclasses Map.
	  * @param entries RDD of Subclass Entries to update
	  * @param subclasses Map containing the subclasses and their path
	  * @return RDD of triples containing wikidata id, instancetype and the path property
	  */
	def updateEntities(
		entries: RDD[SubclassEntry],
		subclasses: Map[String, List[String]]
	): RDD[(String, String, Map[String, List[String]])] = {
		entries
		    .filter(_.data.get(instanceProperty).exists(_.exists(subclasses.contains)))
			.map(updateInstanceOfProperty(_, subclasses))
	}

	/**
	  * Tags the Wikidata entities with their superclass.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val wikiData = input.fromAnyRDD[WikiDataEntity]().head
		val classData = wikiData.map(translateToSubclassEntry)
		val subclassData = classData
			.filter(_.data.contains(subclassProperty))
			.cache
		val subclasses = buildSubclassMap(subclassData, tagClasses)
		val updatedEntities = updateEntities(classData, subclasses)
		List(updatedEntities).toAnyRDD()
	}
}
