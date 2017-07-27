/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.datalake.mock

import de.hpi.ingestion.datalake.models.DLImportEntity
import scala.reflect.runtime.universe._

case class Entity(
	root_value: String,
	data: Map[String, List[String]] = Map()
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
				case _ => Nil
			}
		} else {
			data.getOrElse(attribute, Nil)
		}
	}
}
