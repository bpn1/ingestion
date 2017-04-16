package de.hpi.ingestion.dataimport.dbpedia.models

import scala.reflect.runtime.universe._
import de.hpi.ingestion.datalake.models.DLImportEntity

case class DBPediaEntity(
	dbpedianame: String,
	var wikipageid: Option[String] = None,
	var label: Option[String] = None,
	var description: Option[String] = None,
	var instancetype: Option[String] = None,
	var data: Map[String, List[String]] = Map[String, List[String]]()
) extends DLImportEntity {
	def get(attribute: String): List[String] = {
		if(this.fieldNames[DBPediaEntity].contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "dbpedianame" => List(this.dbpedianame)
				case "wikipageid" | "label" | "description" | "instancetype" => {
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
