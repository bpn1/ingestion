package de.hpi.ingestion.dataimport.dbpedia.models

import de.hpi.ingestion.datalake.models.DLImportEntity

/**
  * Class representing an entity of the DBpediaDump.
  * @param dbpedianame Name of the entity
  * @param wikipageid Id of the wikipedia page
  * @param label Label of the entity
  * @param description Description of the entity
  * @param instancetype Type of the entity
  * @param data all other information about the entity
  */
case class DBpediaEntity(
	dbpedianame: String,
	var wikipageid: Option[String] = None,
	var wikidataid: Option[String] = None,
	var label: Option[String] = None,
	var description: Option[String] = None,
	var instancetype: Option[String] = None,
	var data: Map[String, List[String]] = Map[String, List[String]]()
) extends DLImportEntity {
	def get(attribute: String): List[String] = {
		if(this.fieldNames[DBpediaEntity].contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "dbpedianame" => List(this.dbpedianame)
				case "wikipageid" | "wikidataid" | "label" | "description" | "instancetype" => {
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
