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

package de.hpi.ingestion.datalake.models

import java.util.UUID

object TestData {
	val idList = List.fill(10)(UUID.randomUUID())

	def subject: Subject = Subject(
		id = idList.head,
		master = idList.head,
		datasource = "test",
		properties = Map(
			"key 1" -> List("value 1.1", "value 1.2"),
			"key 2" -> List("value 2.1"),
			"id_implisense" -> List("1"),
			"gen_urls" -> List("http://gen-urls.de"),
			"geo_coords" -> List("0;0")
		)
	)

	def normalizedProperties: Map[String, List[String]] = {
		Map(
			"id_implisense" -> List("1"),
			"gen_urls" -> List("http://gen-urls.de"),
			"geo_coords" -> List("0;0")
		)
	}

	def master: Subject = Subject(
		id = idList.head,
		master = idList.head,
		datasource = "master",
		properties = Map(
			"key 1" -> List("value 1.1", "value 1.2"),
			"key 2" -> List("value 2.1"),
			"id_implisense" -> List("1"),
			"gen_urls" -> List("http://gen-urls.de"),
			"geo_coords" -> List("0;0")
		),
		relations = Map(
			idList(1) -> Map("master" -> "0.5")
		)
	)

	def slave: Subject = Subject(
		id = idList(1),
		master = idList.head,
		datasource = "test",
		properties = Map(
			"key 1" -> List("value 1.3"),
			"key 3" -> List("value 3.1", "value 3.2"),
			"gen_urls" -> List("http://gen-urls.com")
		),
		relations = Map(
			idList.head -> Map("slave" -> "0.5")
		)
	)
}
