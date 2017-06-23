package de.hpi.ingestion.datalake

import java.util.UUID

import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.versioncontrol.VersionDiff.timeFromUUID

/**
  * Manages adding and removing properties of subjects and writes the version fields for changes.
  * @param subject Subject to manage
  * @param templateVersion Version to use for the versioning containing the general information of the version
  */
class SubjectManager(subject: Subject, templateVersion: Version) {

	/**
	  * Creates copy of the template version with a new value and validity or additional data sources.
	  * @param value value to use
	  * @param validity validity to use
	  * @param datasources data source list to append to template Version's
	  * @return new Version containing the new data
	  */
	def makeVersion(
		value: List[String],
		validity: Map[String, String] = Map(),
		datasources: List[String] = Nil
	): Version = {
		templateVersion.copy(
			value = value,
			validity = validity,
			datasources = templateVersion.datasources ++ datasources)
	}

	/**
	  * Sets the name of the Subject with the given validity and data sources.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  * @param datasources List of the data sources that will be appended to Version
	  */
	def setName(value: Option[String], validity: Map[String, String], datasources: List[String]): Unit = {
		if(subject.name != value) {
			subject.name = value
			subject.name_history :+= makeVersion(value.toList, validity, datasources)
		}
	}

	/**
	  * Sets the name of the Subject without validity.
	  * @param value name of the Subject
	  */
	def setName(value: Option[String]): Unit = setName(value, Map[String, String](), Nil)

	/**
	  * Sets the name of the Subject with the given validity.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setName(value: String, validity: Map[String, String] = Map()): Unit = setName(Option(value), validity, Nil)

	/**
	  * Sets the name of the Subject to the value of the given Version and uses its data sources.
	  * @param version: Option of a Version that contains the value and relevant data sources
	  */
	def restoreName(version: Option[Version]): Unit = {
		val (value, validity, dataSources) = extractVersionData(version)
		setName(value.headOption, validity, dataSources)
	}

	/**
	  * Searches the given version's version object or that of a previous version that changed the given field.
	  * @param queryVersionID TimeUUID of the wanted version
	  * @param versionList list of all existing versions of the field (history)
	  * @return version data of the field at the point of the queried version
	  */
	def findVersion(queryVersionID: UUID, versionList: List[Version]): Option[Version] = {
		val queryVersion = versionList.find(_.version == queryVersionID)
		val olderVersions = versionList.filter(v => timeFromUUID(v.version) < timeFromUUID(queryVersionID))
		val olderVersion = olderVersions.sortBy(v => timeFromUUID(v.version)).lastOption
		queryVersion.orElse(olderVersion)
	}

	/**
	  * Extracts value, validity and data sources from given Version.
	  * @param version Option of a version that contains the relevant data
	  * @return Three element tuple of Version value list, validity map and data source list
	  */
	def extractVersionData(version: Option[Version]): (List[String], Map[String, String], List[String]) = {
		val value = version.map(_.value).toList.flatten
		val validity = version.map(_.validity).getOrElse(Map())
		val dataSources = version.map(_.datasources).toList.flatten
		(value, validity, dataSources)
	}

	/**
	  * Restores all data in this subject to the data in the specified Version.
	  * @param version TimeUUID of the version that will be reverted to
	  * @return Modified subject with restored data
	  */
	def restoreVersion(version: UUID): Unit = {
		val nameVersion = findVersion(version, subject.name_history)
		val masterVersion = findVersion(version, subject.master_history)
		val aliasesVersion = findVersion(version, subject.aliases_history)
		val categoryVersion = findVersion(version, subject.category_history)
		val propertiesVersion = subject.properties_history.flatMap { case (prop, values) =>
			findVersion(version, values).map((prop, _))
		}
		val relationsVersion = subject.relations_history
			.mapValues(_.flatMap { case (relationProp, versionList) =>
				findVersion(version, versionList).map((relationProp, _))
			})

		restoreName(nameVersion)
		restoreMaster(masterVersion, relationsVersion)
		restoreAliases(aliasesVersion)
		restoreCategory(categoryVersion)
		restoreProperties(propertiesVersion)
		restoreRelations(relationsVersion)
	}

	/**
	  * Adds a list of aliases to the currently set aliases of the Subject.
	  * @param value List of aliases to add
	  * @param validity validity of the aliases
	  */
	def addAliases(value: List[String], validity: Map[String, String] = Map()): Unit = {
		setAliases(subject.aliases ++ value, validity, Nil)
	}

	/**
	  * Removes all given aliases from the Subject.
	  * @param value List of aliases to remove
	  * @param validity validity of the aliases
	  */
	def removeAliases(value: List[String], validity: Map[String, String] = Map()): Unit = {
		setAliases(subject.aliases.filterNot(value.toSet), validity, Nil)
	}

	/**
	  * Sets the Subjects aliases to the given list of values.
	  * @param value List of aliases the Subject will have
	  * @param validity validity of the aliases
	  */
	def setAliases(
		value: List[String],
		validity: Map[String, String] = Map(),
		datasources: List[String] = Nil
	): Unit = {
		val newAliases = value.distinct
		if(subject.aliases != newAliases) {
			subject.aliases = newAliases
			subject.aliases_history :+= makeVersion(subject.aliases, validity, datasources)
		}
	}

	/**
	  * Sets the aliases of the Subject to the value of the given Version and uses its data sources.
	  * @param version: Option of a Version that contains the value and relevant data sources
	  */
	def restoreAliases(version: Option[Version]): Unit = {
		(setAliases _).tupled(extractVersionData(version))
	}

	/**
	  * Sets the category of the Subject with the given validity.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setCategory(value: Option[String], validity: Map[String, String], datasources: List[String]): Unit = {
		if(subject.category != value) {
			subject.category = value
			subject.category_history :+= makeVersion(value.toList, validity, datasources)
		}
	}

	/**
	  * Sets the category of the Subject with the given validity.
	  * @param value name of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setCategory(value: String, validity: Map[String, String] = Map()): Unit =
		setCategory(Option(value), validity, Nil)

	/**
	  * Sets the category of the Subject without validity.
	  * @param value category of the Subject
	  */
	def setCategory(value: Option[String]): Unit = setCategory(value, Map[String, String](), Nil)

	/**
	  * Sets the category of the Subject to the value of the given Version and uses its data sources.
	  * @param version: Option of a Version that contains the value and relevant data sources
	  */
	def restoreCategory(version: Option[Version]): Unit = {
		val (value, validity, dataSources) = extractVersionData(version)
		setCategory(value.headOption, validity, dataSources)
	}

	/**
	  * Sets the master node of the Subject with the given validity and removes the relation to the old master node.
	  * @param value UUID of the master node of the Subject
	  * @param score score of the relation
	  * @param validity Map containing the data about the validity
	  */
	def setMaster(
		value: Option[UUID],
		score: Double = 1.0,
		validity: Map[String, String] = Map(),
		datasources: List[String] = Nil
	): Unit = {
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
		if(value != subject.master) {
			subject.master_history :+= makeVersion(value.map(_.toString).toList, validity, datasources)
		}
		subject.master = value
	}

	/**
	  * Sets the master node of the Subject with the given validity.
	  * @param value UUID of the master node of the Subject
	  * @param validity Map containing the data about the validity
	  */
	def setMaster(value: UUID, score: Double, validity: Map[String, String]): Unit = {
		setMaster(Option(value), score, validity, Nil)
	}

	/**
	  * Sets the master node of the Subject without validity.
	  * @param value UUID of the master node of the Subject
	  * @param score score of the relation
	  */
	def setMaster(value: UUID, score: Double): Unit = setMaster(value, score, Map[String, String]())

	/**
	  * Sets the master of the Subject to the value of the given Version and uses its data sources.
	  * @param version: Option of a Version that contains the value and relevant data sources
	  */
	def restoreMaster(version: Option[Version], relationsVersions: Map[UUID, Map[String, Version]]): Unit = {
		val (value, validity, dataSources) = extractVersionData(version)
		val masterID = value.headOption.map(UUID.fromString)

		// extract master score from the relevant relation
		val scoreOption = masterID
			.flatMap(relationsVersions.get)
			.flatMap(_.get(SubjectManager.masterKey))
			.flatMap(_.value.headOption)
			.map(_.toDouble)

		scoreOption.foreach(score => setMaster(masterID, score, validity, dataSources))
	}

	/**
	  * Sets the properties of the Subject to the given subjects.
	  * @param value Map of new properties
	  * @param validity validity of each property
	  */
	def setProperties(
		value: Map[String, List[String]],
		validity: Map[String, Map[String, String]] = Map(),
		datasources: Map[String, List[String]] = Map()
	): Unit = {
		val newHistory = (value.keySet ++ subject.properties.keySet)
			.map { prop =>
				val propValues = value.getOrElse(prop, Nil)
				val propSources = datasources.getOrElse(prop, Nil)
				val history = subject.properties_history.getOrElse(prop, Nil)
				val version = List(makeVersion(propValues, validity.getOrElse(prop, Map()), propSources))
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
		if (value.forall(_._2.isEmpty)) return

		val properties = (value.keySet ++ subject.properties.keySet)
			.map { prop =>
				val propValues = subject.properties.getOrElse(prop, Nil) ++ value.getOrElse(prop, Nil)
				prop -> propValues.distinct
			}.filter(_._2.nonEmpty)
			.toMap
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
		setProperties((subject.properties ++ value).filter(_._2.nonEmpty), validity)
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
	  * Sets the properties of the Subject to the value of the given Version and uses its data sources.
	  * @param versions: Map from property name to a Version that contains the value and relevant data sources
	  */
	def restoreProperties(versions: Map[String, Version]): Unit = {
		val restoredValue = versions
			.mapValues(_.value)
			.map(identity)
			.filter(_._2.nonEmpty)

		val restoredValidity = versions
			.mapValues(_.validity)
			.map(identity)
			.filter(_._2.nonEmpty)

		val restoredDataSources = versions
			.mapValues(_.datasources)
			.map(identity)
			.filter(_._2.nonEmpty)

		setProperties(restoredValue, restoredValidity, restoredDataSources)
	}

	/**
	  * Sets the relations of the Subject to the given relations.
	  * @param value Map of the new relations
	  * @param validity validity of each relation
	  */
	def setRelations(
		value: Map[UUID, Map[String, String]],
		validity: Map[UUID, Map[String, Map[String, String]]] = Map(),
		datasources: Map[UUID, Map[String, List[String]]] = Map()
	): Unit = {
		val newHistory = (value.keySet ++ subject.relations.keySet)
			.map { sub =>
				val subRelations = value.getOrElse(sub, Map())
				val subSources = datasources.getOrElse(sub, Map())
				val subOldRelations = subject.relations.getOrElse(sub, Map())
				val subHistory = subject.relations_history.getOrElse(sub, Map())
				val subValidity = validity.getOrElse(sub, Map())
				val newSubHistory = (subRelations.keySet ++ subOldRelations.keySet)
					.map { prop =>
						val propValue = subRelations.get(prop)
						val propSources = subSources.getOrElse(prop, Nil)
						val history = subHistory.getOrElse(prop, Nil)
						val version = List(makeVersion(
							propValue.toList,
							subValidity.getOrElse(prop, Map()), propSources)
						).filter(v => subOldRelations.get(prop) != propValue)
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

	/**
	  * Sets the relations of the Subject to the value of the given Version and uses its data sources.
	  * @param versions: Map from relation target to property name to a Version that contains the value and
	  *                  relevant data sources
	  */
	def restoreRelations(versions: Map[UUID, Map[String, Version]]): Unit = {
		val restoredValue = versions
			.mapValues(_.flatMap { case (prop, version) =>
					version.value.headOption.map((prop, _))
			})
			.map(identity)
			.filter(_._2.nonEmpty)

		val restoredValidity = versions
			.mapValues(_.mapValues(_.validity))
			.map(identity)
			.filter(_._2.nonEmpty)

		val restoredDataSources = versions
			.mapValues(_.mapValues(_.datasources))
			.map(identity)
			.filter(_._2.nonEmpty)

		setRelations(restoredValue, restoredValidity, restoredDataSources)
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
			val duplicateRelation = Map("isDuplicate" -> score.toString)
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
