package de.hpi.ingestion.datalake

import java.util.UUID
import de.hpi.ingestion.datalake.models.{Subject, Version}

/**
  * Manages adding and removing properties of subjects and writes the version fields for changes.
  * @param subject Subject to manage
  * @param templateVersion Version to use for the versioning containing the general information of the version
  */
class SubjectManager(subject: Subject, templateVersion: Version) {

	/**
	  * Creates copy of the template version with a new value and validity.
	  * @param value value to use
	  * @param validity validity to use
	  * @return new Version containing the new data
	  */
	def makeVersion(value: List[String], validity: Map[String, String] = Map()): Version = {
		templateVersion.copy(value = value, validity = validity)
	}

	/**
	  * Sets the name of the Subject with the given validity.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setName(value: Option[String], validity: Map[String, String]): Unit = {
		if(subject.name != value) {
			subject.name = value
			subject.name_history :+= makeVersion(value.toList, validity)
		}
	}

	/**
	  * Sets the name of the Subject without validity.
	  * @param value name of the Subject
	  */
	def setName(value: Option[String]): Unit = setName(value, Map[String, String]())

	/**
	  * Sets the name of the Subject with the given validity.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setName(value: String, validity: Map[String, String] = Map()): Unit = setName(Option(value), validity)

	/**
	  * Adds a list of aliases to the currently set aliases of the Subject.
	  * @param value List of aliases to add
	  * @param validity validity of the aliases
	  */
	def addAliases(value: List[String], validity: Map[String, String] = Map()): Unit = {
		setAliases(subject.aliases ++ value, validity)
	}

	/**
	  * Removes all given aliases from the Subject.
	  * @param value List of aliases to remove
	  * @param validity validity of the aliases
	  */
	def removeAliases(value: List[String], validity: Map[String, String] = Map()): Unit = {
		setAliases(subject.aliases.filterNot(value.toSet), validity)
	}

	/**
	  * Sets the Subjects aliases to the given list of values.
	  * @param value List of aliases the Subject will have
	  * @param validity validity of the aliases
	  */
	def setAliases(value: List[String], validity: Map[String, String] = Map()): Unit = {
		val newAliases = value.distinct
		if(subject.aliases != newAliases) {
			subject.aliases = newAliases
			subject.aliases_history :+= makeVersion(subject.aliases, validity)
		}
	}

	/**
	  * Sets the category of the Subject with the given validity.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setCategory(value: Option[String], validity: Map[String, String]): Unit = {
		if(subject.category != value) {
			subject.category = value
			subject.category_history :+= makeVersion(value.toList, validity)
		}
	}

	/**
	  * Sets the category of the Subject with the given validity.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setCategory(value: String, validity: Map[String, String] = Map()): Unit = setCategory(Option(value), validity)

	/**
	  * Sets the category of the Subject without validity.
	  * @param value category of the Subject
	  */
	def setCategory(value: Option[String]): Unit = setCategory(value, Map[String, String]())

	/**
	  * Sets the master node of the Subject with the given validity and removes the relation to the old master node.
	  * @param value UUID of the master node of the Subject
	  * @param score score of the relation
	  * @param validity Map containing the data about the validity
	  */
	def setMaster(value: Option[UUID], score: Double = 1.0, validity: Map[String, String] = Map()): Unit = {
		val higherScore = subject.master
			.flatMap(subject.relations.get)
			.flatMap(_.get(SubjectManager.slaveKey))
			.exists(_.toDouble < score)
		subject.master
			.filterNot(value.contains)
			.foreach(masterId => removeRelations(Map(masterId -> List(SubjectManager.slaveKey))))
		value
			.filter(!subject.master.contains(_) || higherScore)
			.foreach(newMasterId =>
				addRelations(
					Map(newMasterId -> SubjectManager.slaveRelation(score)),
					Map(newMasterId -> Map(SubjectManager.slaveKey -> validity))))
		if(value != subject.master) subject.master_history :+= makeVersion(value.map(_.toString).toList, validity)
		subject.master = value
	}

	/**
	  * Sets the master node of the Subject with the given validity.
	  * @param value UUID of the master node of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setMaster(value: UUID, score: Double, validity: Map[String, String]): Unit = {
		setMaster(Option(value), score, validity)
	}

	/**
	  * Sets the master node of the Subject without validity.
	  * @param value UUID of the master node of the Subject
	  * @param score score of the relation
	  */
	def setMaster(value: UUID, score: Double): Unit = setMaster(value, score, Map[String, String]())

	/**
	  * Sets the properties of the Subject to the given subjects.
	  * @param value Map of new properties
	  * @param validity validity of each property
	  */
	def setProperties(value: Map[String, List[String]], validity: Map[String, Map[String, String]] = Map()): Unit = {
		val newHistory = (value.keySet ++ subject.properties.keySet)
			.map { prop =>
				val propValues = value.getOrElse(prop, Nil)
				val history = subject.properties_history.getOrElse(prop, Nil)
				val version = List(makeVersion(propValues, validity.getOrElse(prop, Map())))
					.filter(v => !subject.properties.get(prop).contains(propValues))
				(prop, history ++ version)
			}.toMap
		subject.properties = value
		subject.properties_history ++= newHistory
	}

	/**
	  * Adds a list of properties and their values to the Subject and writes the corresponding Version fields.
	  * @param value Map of properties and their values
	  * @param validity validity of each property
	  */
	def addProperties(value: Map[String, List[String]], validity: Map[String, Map[String, String]] = Map()): Unit = {
		val properties = (value.keySet ++ subject.properties.keySet)
			.map { prop =>
				val propValues = subject.properties.getOrElse(prop, Nil) ++ value.getOrElse(prop, Nil)
				(prop, propValues.distinct)
			}.toMap
		setProperties(properties, validity)
	}

	/**
	  * Adds a list of properties and their values to the Subject, overwrites existing values and writes the
	  * corresponding Version fields.
	  * @param value Map of properties and their values
	  * @param validity validity of each property
	  */
	def overwriteProperties(
		value: Map[String, List[String]],
		validity: Map[String, Map[String, String]] = Map()
	): Unit = {
		setProperties(subject.properties ++ value, validity)
	}

	/**
	  * Clears all property fields of the Subject and adds the clearing of these to their history.
	  * @param validity validity of the removal of the properties
	  */
	def clearProperties(validity: Map[String, String] = Map()): Unit = {
		setProperties(Map(), subject.properties.keySet.map((_, validity)).toMap)
	}

	/**
	  * Removes a given List of properties from the Subject.
	  * @param value List of properties to remove
	  * @param validity validity of the properties
	  */
	def removeProperties(value: List[String], validity: Map[String, Map[String, String]] = Map()): Unit = {
		setProperties(subject.properties -- value, validity)
	}

	/**
	  * Sets the relations of the Subject to the given relations
	  * @param value Map of the new relations
	  * @param validity validity of each relation
	  */
	def setRelations(
		value: Map[UUID, Map[String, String]],
		validity: Map[UUID, Map[String, Map[String, String]]] = Map()
	): Unit = {
		val newHistory = (value.keySet ++ subject.relations.keySet)
			.map { sub =>
				val subRelations = value.getOrElse(sub, Map())
				val subOldRelations = subject.relations.getOrElse(sub, Map())
				val subHistory = subject.relations_history.getOrElse(sub, Map())
				val subValidity = validity.getOrElse(sub, Map())
				val newSubHistory = (subRelations.keySet ++ subOldRelations.keySet)
					.map { prop =>
						val propValue = subRelations.get(prop)
						val history = subHistory.getOrElse(prop, Nil)
						val version = List(makeVersion(propValue.toList, subValidity.getOrElse(prop, Map())))
							.filter(v => subOldRelations.get(prop) != propValue)
						(prop, history ++ version)
					}.toMap
				(sub, subHistory ++ newSubHistory)
			}.toMap
		subject.relations = value
		subject.relations_history ++= newHistory
	}

	/**
	  * Adds a list of relations to the Subject.
	  * @param value Map of the new relations
	  * @param validity validity of each relation
	  */
	def addRelations(
		value: Map[UUID, Map[String, String]],
		validity: Map[UUID, Map[String, Map[String, String]]] = Map()
	): Unit = {
		val relations = (value.keySet ++ subject.relations.keySet)
			.map { sub => (sub, subject.relations.getOrElse(sub, Map()) ++ value.getOrElse(sub, Map())) }
			.toMap
		setRelations(relations, validity)
	}

	/**
	  * Clears all relation fields of the Subject and adds the clearing of these to their history.
	  * @param validity validity of the removal of the relations
	  */
	def clearRelations(validity: Map[String, String] = Map()): Unit = {
		val completeValidity = subject.relations.map { case (id, values) =>
			(id, values.mapValues(v => validity).map(identity))
		}
		setRelations(Map(), completeValidity)
	}

	/**
	  * Removes a given List of relations from the Subject.
	  * @param value List of UUIDs to wich the relations will be removed
	  */
	def removeRelations(value: List[UUID]): Unit = {
		removeRelations(value, Map[UUID, Map[String, Map[String, String]]]())
	}

	/**
	  * Removes a given List of relations from the Subject.
	  * @param value List of UUIDs to wich the relations will be removed
	  * @param validity validity of the relations
	  */
	def removeRelations(value: List[UUID], validity: Map[UUID, Map[String, Map[String, String]]]): Unit = {
		setRelations(subject.relations -- value, validity)
	}

	/**
	  * Removes a given List of relations properties from the Subject.
	  * @param value Map of UUIDs and the relation properties to remove
	  * @param validity validity of the relations
	  */
	def removeRelations(
		value: Map[UUID, List[String]],
		validity: Map[UUID, Map[String, Map[String, String]]] = Map()
	): Unit = {
		val relations = subject.relations
			.map { case (sub, relationProps) =>
				(sub, relationProps -- value.getOrElse(sub, Nil))
			}.filter(_._2.nonEmpty)
		setRelations(relations, validity)
	}
}

/**
  * Companion-object for the SubjectManager class.
  */
object SubjectManager {
	val slaveKey = "slave"
	val masterKey = "master"

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

	/**
	  * Returns default relation Subject have to their master nodes.
	  * @param score score of the slave relation
	  * @return Map containing the data of the default slave master relation
	  */
	def slaveRelation(score: Double): Map[String, String] = Map(slaveKey -> score.toString)

	/**
	  * Returns default relation Subject have to their slave nodes.
	  * @param score score of the master relation
	  * @return Map containing the data of the default master slave relation
	  */
	def masterRelation(score: Double): Map[String, String] = Map(masterKey -> score.toString)
}
