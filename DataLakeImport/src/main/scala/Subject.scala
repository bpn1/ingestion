package DataLake

import java.util.{UUID, Date}
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
	def toMap: Map[String, Any] = Map(
		"id" -> id,
		"name" -> name,
		"aliases" -> aliases,
		"category" -> category,
		"properties" -> properties,
		"relations" -> relations
	)
}