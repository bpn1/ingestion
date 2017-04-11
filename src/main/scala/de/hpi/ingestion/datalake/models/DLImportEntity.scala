package de.hpi.ingestion.datalake.models

trait DLImportEntity extends Serializable {
	/**
	  * Returns the field names of this class.
	  * @return list of field names
	  */
	def fieldNames(): List[String]

	/**
	  * Returns the values of an attribute given the name of the attribute.
	  * @param attribute name of the attribute to retrieve
	  * @return list of the attribute values
	  */
	def get(attribute: String): List[String]
}
