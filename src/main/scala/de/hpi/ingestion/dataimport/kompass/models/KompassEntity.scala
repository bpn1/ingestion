package de.hpi.ingestion.dataimport.kompass.models

import de.hpi.ingestion.datalake.models.DLImportEntity

/**
  * Class representing an entity of the KompassDump.
  * @param name ???
  * @param url ???
  * @param sector ???
  * @param specified_sector ???
  * @param district ???
  * @param data all other information about the entity
  */
case class KompassEntity(
	name: String,
	var url: Option[String] = None,	// was ist in Kompass so drin?
	var sector: Option[String] = None,
	var specified_sector: Option[String] = None,
	var county: Option[String] = None,
	var district: Option[String] = None,
	var data: Map[String, List[String]] = Map[String, List[String]]()	//given?
) extends DLImportEntity {
	def get(attribute: String): List[String] = {
		if(this.fieldNames[KompassEntity].contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "name" => List(this.name)
				case "url" | "sector" | "specified_sector" | "county" | "district" => {
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
