package de.hpi.ingestion.dataimport.wikidata

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

import scala.collection.mutable
import org.apache.spark.rdd._

import scala.language.postfixOps
import de.hpi.ingestion.dataimport.wikidata.models.{SubclassEntry, WikiDataEntity}

/**
  * This job builds the subclass hierarchy of selected Wikidata classes and tags every Wikidata entity, that is an
  * instance of one of the subclasses, with the top level class.
  */
object TagEntities {
	val keyspace = "wikidumps"
	val tablename = "wikidata"
	val tagClasses = Map(
		"Q43229" -> "organization",
		"Q4830453" -> "business",
		"Q268592" -> "economic branch")
	val instanceProperty = "instance of"
	val subclassProperty = "subclass of"
	val wikidataPathProperty = "wikidata_path"

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
		subclasses ++ oldClasses.filter { case (key, path) =>
			val oldPathLength = subclasses.get(key).map(_.size)
			oldPathLength.isEmpty || path.size < oldPathLength.get
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
					.map { entry =>
						entry.classList = entry.data(subclassProperty).filter(oldClasses.contains)
						entry
					}.filter(_.classList.nonEmpty)
					.map(entry => (entry.id, oldClasses(entry.classList.head) ++ List(entry.label)))
					.collect
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
	  * Returns true of the entry is an instance of one of the given classes.
	  * @param entry Subclass entry to test
	  * @param classes map of class data used to test the entry
	  * @return true if one of the values of the instance of property of the entry exists as key
	  * in the class map
	  */
	def isInstanceOf(entry: SubclassEntry, classes: Map[String, List[String]]): Boolean = {
		entry.data.contains(instanceProperty) &&
			entry.data(instanceProperty).exists(classes.contains)
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
		(entry.id, path.head, Map(wikidataPathProperty -> path))
	}

	def main(args : Array[String]): Unit = {
		val conf = new SparkConf()
			.setAppName("TagEntities")

		val sc = new SparkContext(conf)
		val classData = sc.cassandraTable[WikiDataEntity](keyspace, tablename)
			.map(translateToSubclassEntry)

		val subclassData = classData
			.filter(_.data.contains(subclassProperty))
			.cache
		val subclasses = buildSubclassMap(subclassData, tagClasses)
		val updatedEntities = classData
			.filter(isInstanceOf(_, subclasses))
			.map(updateInstanceOfProperty(_, subclasses))

		updatedEntities.saveToCassandra(
			keyspace,
			tablename,
			SomeColumns("id", "instancetype", "data" append))
	}
}
