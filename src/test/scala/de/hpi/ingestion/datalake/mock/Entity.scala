package de.hpi.ingestion.datalake.mock

import de.hpi.ingestion.datalake.models.DLImportEntity
import scala.reflect.runtime.universe._

case class Entity(
	root_value: String,
	data: Map[String, List[String]] = Map[String, List[String]]()
) extends DLImportEntity {
	def fieldNames(): List[String] = {
		def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
			case m: MethodSymbol if m.isCaseAccessor => m
		}.toList
		val accessors = classAccessors[Entity]
		accessors.map(_.name.toString)
	}
	def get(attribute: String): List[String] = {
		if(this.fieldNames.contains(attribute)) {
			val field = this.getClass.getDeclaredField(attribute)
			field.setAccessible(true)
			attribute match {
				case "root_value" => List(this.root_value)
				case _ => List[String]()
			}
		} else {
			data.getOrElse(attribute, List[String]())
		}
	}
}
