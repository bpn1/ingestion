package de.hpi.ingestion.dataimport.kompass.models

import java.util.UUID

import de.hpi.ingestion.datalake.models.DLImportEntity

/**
  * Class representing an entity of the KompassDump.
  * @param name title of page in Kompass data
  * @param data all other information about the entity
  */
case class KompassEntity(
	var name: Option[String] = None,
	var instancetype: Option[String] = None,
	var data: Map[String, List[String]] = Map[String, List[String]](),
	id: UUID = UUID.randomUUID()
) extends DLImportEntity {
	def get(attribute: String): List[String] = {
		if(this.fieldNames[KompassEntity]().contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "id" | "name" => {
					val value = field.get(this).asInstanceOf[Option[String]]
					value.map(List(_)).getOrElse(Nil)
				}
				case _ => Nil
			}
		} else {
			data.getOrElse(attribute, Nil)
		}
	}
}
