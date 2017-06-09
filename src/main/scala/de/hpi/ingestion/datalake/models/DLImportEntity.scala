package de.hpi.ingestion.datalake.models

import scala.reflect.runtime.universe._

trait DLImportEntity extends Serializable {
	/**
	  * Returns the field names of this class.
	  * @return list of field names
	  */
	protected def fieldNames[A <: DLImportEntity : TypeTag](): List[String] = {
		def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
			case m: MethodSymbol if m.isCaseAccessor => m
		}.toList
		val accessors = classAccessors[A]
		accessors.map(_.name.toString)
	}

	/**
	  * Returns the values of an attribute given the name of the attribute.
	  * @param attribute name of the attribute to retrieve
	  * @return list of the attribute values
	  */
	def get(attribute: String): List[String]
}
