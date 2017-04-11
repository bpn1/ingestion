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

	def fieldNames(): List[String] = {
		def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
			case m: MethodSymbol if m.isCaseAccessor => m
		}.toList
		val accessors = classAccessors[DBPediaEntity]
		accessors.map(_.name.toString)
	}

	def get(attribute: String): List[String] = {
		if(this.fieldNames.contains(attribute)) {
			val field = this.getClass().getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "dbpedianame" => List(this.dbpedianame)
				case "wikipageid" | "label" | "description" | "instancetype" => {
					val value = field.get(this).asInstanceOf[Option[String]]
					List[String](value.getOrElse(""))
				}
				case _ => List[String]()
			}
		} else {
			data.getOrElse(attribute, List[String]())
		}
	}
}
