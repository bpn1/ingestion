package de.hpi.ingestion.dataimport.dbpedia

import scala.util.matching.Regex

object DBpediaNormalizeStrategy extends Serializable {

	implicit class Regex(sc: StringContext) {
		def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
	}

	def normalizeEmployees(values: List[String]): List[String] = {
		values.map {
			case r"""(\d+)${number}..xsd:.+""" => number
			case r"""über (\d+)${number}@de \.""" => number
			case _ => ""
		}
	}

	def normalizeNothing(values: List[String]): List[String] = values

	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_employees" => normalizeEmployees
			case _ => normalizeNothing
		}
	}

}
