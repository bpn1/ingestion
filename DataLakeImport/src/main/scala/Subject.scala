package DataLake

import scala.reflect.runtime.universe._
import java.util.{Date, UUID}

import com.datastax.driver.core.utils.UUIDs

case class Version(
	version: UUID,
	program: String,
	value: List[String],
	validity: Map[String, String],
	datasources: List[String],
	timestamp: Date
)

case class Subject(
	// main data attributes
	var id: UUID = UUIDs.random(),
	var name: Option[String] = None,
	var aliases: List[String] = List[String](),
	var category: Option[String] = None,
	var properties: Map[String, List[String]] = Map[String, List[String]](),
	var relations: Map[UUID, Map[String, String]]  = Map[UUID, Map[String, String]](),

	// history attributes
	var name_history: List[Version] = List[Version](),
	var aliases_history: List[Version] = List[Version](),
	var category_history: List[Version] = List[Version](),
	var properties_history: Map[String, List[Version]] = Map[String, List[Version]](),
	var relations_history:
		Map[UUID, Map[String, List[Version]]] = Map[UUID, Map[String, List[Version]]]()
){
	override def equals(obj: Any): Boolean = obj match {
		case that: Subject => that.id == this.id
		case _ => false
	}

	override def hashCode(): Int = this.id.hashCode()

	def fieldNames(): List[String] = {
		def classAccessors[T: TypeTag]: List[MethodSymbol] = typeOf[T].members.collect {
			case m: MethodSymbol if m.isCaseAccessor => m
		}.toList
		val accessors = classAccessors[Subject]
		accessors.map(_.name.toString)
	}

	// use reflection to get attribute from a string
	def get(attribute: String): List[String] = {
		if(this.fieldNames.contains(attribute)) {
			val field = this.getClass().getDeclaredField(attribute)
			field.setAccessible(true)
			if(attribute == "name" || attribute == "category") {
				val value = field.get(this).asInstanceOf[Option[String]]
				List[String](value.getOrElse(""))
			} else if(attribute == "aliases") {
				field.get(this).asInstanceOf[List[String]]
			} else {
				List[String]()
			}
		} else {
			properties.getOrElse(attribute, List[String]())
		}
	}
}
