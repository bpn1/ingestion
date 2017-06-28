package de.hpi.ingestion.datalake.models

import java.util.UUID

import de.hpi.ingestion.datalake.SubjectManager

case class Old_Subject(
	var id: UUID = UUID.randomUUID(),
	var name: Option[String] = None,
	var aliases: List[String] = Nil,
	var category: Option[String] = None,
	var properties: Map[String, List[String]] = Map(),
	var relations: Map[UUID, Map[String, String]] = Map(),
	var master: Option[UUID] = None,

	var name_history: List[Version] = Nil,
	var aliases_history: List[Version] = Nil,
	var category_history: List[Version] = Nil,
	var properties_history: Map[String, List[Version]] = Map(),
	var relations_history: Map[UUID, Map[String, List[Version]]] = Map(),
	var master_history: List[Version] = Nil
) extends DLImportEntity {
	// $COVERAGE-OFF$
	/**
	  * Compares the subject using its uuid
	  * @param obj object to compare this subject to
	  * @return whether the subject equals the other object
	  */
	override def equals(obj: Any): Boolean = obj match {
		case that: Old_Subject => that.id == this.id
		case _ => false
	}

	/**
	  * Returns the hash code of the subjects uuid.
	  * @return hash code of the id
	  */
	override def hashCode(): Int = this.id.hashCode()

	/**
	  * Returns the values of an attribute given the name of the attribute.
	  * @param attribute name of the attribute to retrieve
	  * @return list of the attribute values
	  */
	def get(attribute: String): List[String] = {
		if(this.fieldNames[Old_Subject]().contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "name" | "category" => field.get(this).asInstanceOf[Option[String]].toList
				case "aliases" => field.get(this).asInstanceOf[List[String]]
				case "master" => field.get(this).asInstanceOf[Option[UUID]].map(_.toString).toList
				case _ => Nil
			}
		} else {
			properties.getOrElse(attribute, Nil)
		}
	}

	/**
	  * Returns only the normalized properties of this Subject.
	  * @return subset of the properties Map containing only the normalized properties
	  */
	def normalizedProperties(): Map[String, List[String]] = {
		properties.filterKeys(Subject.normalizedPropertyKeys)
	}

	/**
	  * Returns the score of the slave relation to this Subjects master node if it has one.
	  * @return score of the master node relation
	  */
	def masterScore(): Option[Double] = {
		master
			.flatMap(relations.get)
			.flatMap(_.get(SubjectManager.slaveKey))
			.map(_.toDouble)
	}
	// $COVERAGE-ON$
}


