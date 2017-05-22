package de.hpi.ingestion.datalake.models

import java.util.UUID
import scala.collection.mutable

case class Subject(
	var id: UUID = UUID.randomUUID(),
	var name: Option[String] = None,
	var aliases: List[String] = List[String](),
	var category: Option[String] = None,
	var properties: Map[String, List[String]] = Map[String, List[String]](),
	var relations: Map[UUID, Map[String, String]]  = Map[UUID, Map[String, String]](),

	var name_history: List[Version] = List[Version](),
	var aliases_history: List[Version] = List[Version](),
	var category_history: List[Version] = List[Version](),
	var properties_history: Map[String, List[Version]] = Map[String, List[Version]](),
	var relations_history:
		Map[UUID, Map[String, List[Version]]] = Map[UUID, Map[String, List[Version]]]()
) extends DLImportEntity {
	/**
	  * Compares the subject using its uuid
	  * @param obj object to compare this subject to
	  * @return whether the subject equals the other object
	  */
	override def equals(obj: Any): Boolean = obj match {
		case that: Subject => that.id == this.id
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
		if(this.fieldNames[Subject]().contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "name" | "category" => {
					val value = field.get(this).asInstanceOf[Option[String]]
					value.map(List(_)).getOrElse(Nil)
				}
				case "aliases" => field.get(this).asInstanceOf[List[String]]
				case _ => Nil
			}
		} else {
			properties.getOrElse(attribute, Nil)
		}
	}
}
