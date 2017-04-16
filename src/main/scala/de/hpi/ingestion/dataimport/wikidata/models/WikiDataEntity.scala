package de.hpi.ingestion.dataimport.wikidata.models

import de.hpi.ingestion.datalake.models.DLImportEntity
import scala.reflect.runtime.universe._

case class WikiDataEntity(
	id: String,
	var aliases: List[String] = List[String](),
	var description: Option[String] = None,
	var entitytype: Option[String] = None,
	var wikiname: Option[String] = None,
	var enwikiname: Option[String] = None,
	var instancetype: Option[String] = None,
	var label: Option[String] = None,
	var data: Map[String, List[String]] = Map[String, List[String]]()
) extends DLImportEntity {
	def get(attribute: String): List[String] = {
		if(this.fieldNames[WikiDataEntity].contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "id" => List(this.id)
				case "description" | "entititype" | "wikiname" | "enwikiname" | "instancetype" | "label" => {
					val value = field.get(this).asInstanceOf[Option[String]]
					value.map(List(_)).getOrElse(Nil)
				}
				case "aliases" => field.get(this).asInstanceOf[List[String]]
				case _ => Nil
			}
		} else {
			data.getOrElse(attribute, Nil)
		}
	}
}
