package de.hpi.ingestion.datalake.models

import scala.reflect.runtime.universe._
import java.util.{Date, UUID}

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
){
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
	  * Returns the field names of this class.
	  * @return list of field names
	  */
	def fieldNames(): List[String] = {
		def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
			case m: MethodSymbol if m.isCaseAccessor => m
		}.toList
		val accessors = classAccessors[Subject]
		accessors.map(_.name.toString)
	}

	/**
	  * Returns the values of an attribute given the name of the attribute.
	  * @param attribute name of the attribute to retrieve
	  * @return list of the attribute values
	  */
	def get(attribute: String): List[String] = {
		if(this.fieldNames.contains(attribute)) {
			val field = this.getClass().getDeclaredField(attribute)
			field.setAccessible(true)
			if(attribute == "name" || attribute == "category") {
				val value = field.get(this).asInstanceOf[Option[String]]
				List[String](value.getOrElse(""))
			} else if(attribute == "aliases") {
				field.get(this).asInstanceOf[List[String]]
			} else {
				List[String]()
			}
		} else {
			properties.getOrElse(attribute, List[String]())
		}
	}
}
