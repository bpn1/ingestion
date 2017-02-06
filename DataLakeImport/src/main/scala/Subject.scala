package DataLake

import java.lang.reflect.Field
import java.util.{Date, UUID}

import com.datastax.spark.connector.types._
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
	var relations_history: Map[UUID, Map[String, List[Version]]]  = Map[UUID, Map[String, List[Version]]]()
){

  // use reflection to get attribute from a string
  def get[T](attribute: String): T = {
    val field: Field = this.getClass().getDeclaredField(attribute)
    field.get(this) match {
			case Some(value) => value.asInstanceOf[T]
			case None => properties(attribute).asInstanceOf[T]
		}
  }
}