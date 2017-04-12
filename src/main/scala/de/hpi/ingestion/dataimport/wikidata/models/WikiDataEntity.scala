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
	def fieldNames(): List[String] = {
		def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
			case m: MethodSymbol if m.isCaseAccessor => m
		}.toList
		val accessors = classAccessors[WikiDataEntity]
		accessors.map(_.name.toString)
	}

	def get(attribute: String): List[String] = {
		if(this.fieldNames.contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case _ => List[String]()
			}
		} else {
			data.getOrElse(attribute, List[String]())
		}
	}
}
