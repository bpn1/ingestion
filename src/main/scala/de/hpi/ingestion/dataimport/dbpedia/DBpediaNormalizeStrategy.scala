package de.hpi.ingestion.dataimport.dbpedia

object DBpediaNormalizeStrategy extends Serializable {

	implicit class Regex(sc: StringContext) {
		def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
	}	// from http://stackoverflow.com/questions/4636610/how-to-pattern-match-using-regular-expression-in-scala

	def normalizeEmployees(values: List[String]): List[String] = {
		values.map {
			case r"""(\d+)${number}..xsd:.+""" => number
			case r"""über (\d+)${number}@de \.""" => number
			case other => other
		}
	}

	def normalizeCountry(values: List[String]): List[String] = {
		values.map {
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß-]+)${country}""" => country
			case r"""([A-Za-zÄäÖöÜüß-]+)${country}@de \.""" => country
			case other => other
		}
		//"Deutschland@de .", "dbpedia-de:England"
	}

	def normalizeCoords(values: List[String]): List[String] = {
		values.map {
			case r"""\d+..xsd:integer""" => "filter_me"
			case r"""(\d+\.\d+)${ord}..xsd:.+""" => ord
			case other => other
		}.filterNot(_ == "filter_me").grouped(2).toList.distinct.flatten
	}

	def normalizeNothing(values: List[String]): List[String] = values

	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_employees" => normalizeEmployees
			case "geo_country" => normalizeCountry
			case "geo_coords" => normalizeCoords
			case _ => normalizeNothing
		}
	}

}
