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

// scalastyle:off method.length
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

	def exportSubjects: List[Subject] = {
		List(
			Subject(
				id = UUID.fromString("224e1a50-13e2-11e7-9a30-7384674b582f"),
				master = UUID.fromString("224e1a50-13e2-11e7-9a30-7384674b582f"),
				datasource = "implisense",
				category = Option("business"),
				name = Option("Testunternehmen 1"),
				aliases = List("Unternehmen 1"),
				properties = Map(
					"cr_active" -> List("0"),
					"cr_court" -> List("1"),
					"cr_number" -> List("2"),
					"cr_ids" -> List("3"),
					"cr_formerCourt" -> List("4"),
					"cr_type" -> List("5"),
					"id_implisense" -> List("6"),
					"id_wikidata" -> List("7"),
					"id_dbpedia" -> List("8"),
					"id_kompass" -> List("9"),
					"id_vat" -> List("10"),
					"id_lei" -> List("11"),
					"id_ebid" -> List("12"),
					"id_buergel" -> List("13"),
					"id_wikipedia" -> List("14"),
					"id_freebase" -> List("15"),
					"id_wikimeda_commons" -> List("16"),
					"id_geonames" -> List("17"),
					"id_viaf" -> List("18"),
					"id_lccn" -> List("19"),
					"id_tax" -> List("20"),
					"sm_youtube" -> List("21"),
					"sm_googleplus" -> List("22"),
					"sm_xing" -> List("23"),
					"sm_linkedin" -> List("24"),
					"sm_instagram" -> List("25"),
					"sm_pinterest" -> List("26"),
					"sm_kununu" -> List("27"),
					"sm_flickr" -> List("28"),
					"sm_github" -> List("29"),
					"sm_slideshare" -> List("30"),
					"sm_foursquare" -> List("31"),
					"sm_twitter" -> List("32"),
					"sm_facebook" -> List("33"),
					"geo_postal" -> List("34"),
					"geo_street" -> List("35"),
					"geo_city" -> List("36"),
					"geo_country" -> List("37"),
					"geo_county" -> List("38"),
					"geo_coords" -> List("39"),
					"geo_quality" -> List("40"),
					"date_founding" -> List("41"),
					"gen_size" -> List("42"),
					"gen_employees" -> List("43"),
					"gen_sectors" -> List("44"),
					"gen_urls" -> List("45"),
					"gen_phones" -> List("46"),
					"gen_emails" -> List("47"),
					"gen_legal_form" -> List("48"),
					"gen_founder" -> List("49"),
					"gen_description" -> List("50"),
					"gen_turnover" -> List("51"),
					"gen_ceo" -> List("52"),
					"gen_capital" -> List("53"),
					"gen_management" -> List("54"),
					"gen_revenue" -> List("55"),
					"gen_sales" -> List("56"))
			),
			Subject(
				id = UUID.fromString("324e1a50-13e2-11e7-9a30-7384674b582f"),
				master = UUID.fromString("324e1a50-13e2-11e7-9a30-7384674b582f"),
				datasource = "implisense",
				category = Option("organization"),
				aliases = List("Unternehmen 2", "Testunternehmen 2"),
				properties = Map(
					"cr_active" -> List("0"),
					"id_vat" -> List("10"),
					"id_tax" -> List("20"),
					"sm_slideshare" -> List("30"),
					"geo_quality" -> List("40"),
					"gen_description" -> List("50"))
			)
		)
	}

	def tsvSubjects: List[String] = {
		val quote = "\""
		List(
			s"${quote}224e1a50-13e2-11e7-9a30-7384674b582f$quote\t${quote}Testunternehmen 1$quote\t" +
				s"${quote}Unternehmen 1$quote\t${quote}business$quote\t" +
				(0 to 56).map(value => s"$quote$value$quote").mkString("\t"),
			s"${quote}324e1a50-13e2-11e7-9a30-7384674b582f$quote\t\t${quote}Unternehmen 2$quote," +
				s"${quote}Testunternehmen 2$quote\t${quote}organization$quote\t" +
				(0 to 56).map(value => s"$quote$value$quote".filter(c => value % 10 == 0)).mkString("\t")
		)
	}
}
// scalastyle:on method.length
