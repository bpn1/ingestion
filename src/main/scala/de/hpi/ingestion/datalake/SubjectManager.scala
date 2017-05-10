package de.hpi.ingestion.datalake

import scala.collection.mutable
import java.util.UUID
import de.hpi.ingestion.datalake.models.{Subject, Version}

class SubjectManager(subject: Subject, templateVersion: Version) {
	def makeVersion(value: List[String], validity: Map[String, String] = null): Version = {
		templateVersion.copy(value = value, validity = validity)
	}

	def setName(value: String, validity: Map[String, String] = null) {
		subject.name = Option(value)
		subject.name_history = List(makeVersion(List(value), validity))
	}

	def addAliases(value: List[String], validity: Map[String, String] = null) {
		subject.aliases = value ++ subject.aliases
		subject.aliases_history = List(makeVersion(subject.aliases, validity))
	}

	def setCategory(value: String, validity: Map[String, String] = null) {
		subject.category = Option(value)
		subject.category_history = List(makeVersion(List(value), validity))
	}

	def addProperties(
		value: Map[String, List[String]],
		validityMap: Map[String, Map[String, String]] = null
	) {
		val buffer = mutable.Map[String, List[String]]()
		buffer ++= subject.properties
		val historyBuffer = mutable.Map[String, List[Version]]()
		historyBuffer ++= subject.properties_history

		// add a history entry and do deduplication for every field
		for((key, list) <- value) {
			val oldList = buffer.getOrElseUpdate(key, List[String]())
			// check if the lists contain the same elements
			if(oldList.toSet != list.toSet) {
				buffer(key) = (oldList ++ list).distinct
				val oldHistory = historyBuffer.getOrElseUpdate(key, List[Version]())

				var validity: Map[String, String] = null
				if(validityMap != null) {
					validity = validityMap.getOrElse(key, null)
				}
				historyBuffer(key) = oldHistory ++ List(makeVersion(buffer(key), validity))
			}
		}
		subject.properties = buffer.toMap
		subject.properties_history = historyBuffer.toMap
	}

	def removeProperties(): Unit = {
		val historyBuffer = mutable.Map[String, List[Version]]()
		historyBuffer ++= subject.properties_history

		for(key <- subject.properties.keys) {
			val oldHistory = historyBuffer.getOrElseUpdate(key, List[Version]())
			historyBuffer.update(key, oldHistory ++ List(makeVersion(Nil)))
		}

		subject.properties = Map[String, List[String]]()
		subject.properties_history = historyBuffer.toMap
	}

	def addRelations(
		relations: Map[UUID, Map[String, String]],
		validityMap: Map[UUID, Map[String, Map[String, String]]] = null
	): Unit = {
		val buffer = mutable.Map[UUID, Map[String, String]]()
		buffer ++= subject.relations

		val historyBuffer = mutable.Map[UUID, Map[String, List[Version]]]()
		historyBuffer ++= subject.relations_history

		// add a history entry for every field
		for((targetID, properties) <- relations) {
			val valueBuffer = mutable.Map[String, String]()
			val versionBuffer = mutable.Map[String, List[Version]]()

			for((key, value) <- properties) {
				val oldValue = buffer
					.getOrElseUpdate(targetID, Map[String, String]())
					.getOrElse(key, null)

				if(value != oldValue) {
					val oldHistory = historyBuffer
						.getOrElse(targetID, mutable.Map[String, List[Version]]())
						.getOrElse(key, List[Version]())
					valueBuffer(key) = value

					var validity: Map[String, String] = null
					if(validityMap != null) {
						validity = validityMap
							.getOrElse(targetID, Map[String, Map[String, String]]())
							.getOrElse(key, null)
					}
					versionBuffer(key) = oldHistory ++ List(makeVersion(List(value), validity))
				}
			}
			buffer(targetID) = valueBuffer.toMap
			historyBuffer(targetID) = versionBuffer.toMap
		}
		subject.relations = buffer.toMap
		subject.relations_history = historyBuffer.toMap
	}
}

/**
  * Companion-object for the SubjectManager class.
  */
object SubjectManager {

	/**
	  * Adds symmetric relations between two subjects.
	  * @param subject1 first subject
	  * @param subject2 second subject
	  * @param relations Map containing the attribute keys and values describing the relation, typically a 'type' value
	  *                  should be passed
	  * @param version Version of the current job used for versioning
	  */
	def addSymRelation(subject1: Subject, subject2: Subject, relations: Map[String,String], version: Version): Unit = {
		val subjectManager1 = new SubjectManager(subject1, version)
		val subjectManager2 = new SubjectManager(subject2, version)

		subjectManager1.addRelations(Map(subject2.id -> relations))
		subjectManager2.addRelations(Map(subject1.id -> relations))
	}

	/**
	  * Creates symmetric duplicate relations between all found duplicates and thus an SCC between multiple duplicates.
	  * @param duplicates List of duplicate three-tuples in the form (Subject1, Subject2, score)
	  * @param version Version of the current job used for versioning
	  */
	def buildDuplicatesSCC(duplicates: List[(Subject, Subject, Double)], version: Version): Unit = {
		duplicates.foreach { case (subject1, subject2, score) =>
			val duplicateRelation = Map("type" -> "isDuplicate", "confidence" -> score.toString)
			addSymRelation(subject1, subject2, duplicateRelation, version)
		}
	}
}
