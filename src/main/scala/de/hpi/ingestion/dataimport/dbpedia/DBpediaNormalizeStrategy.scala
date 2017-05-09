package de.hpi.ingestion.dataimport.dbpedia

object DBpediaNormalizeStrategy extends Serializable {

	implicit class Regex(sc: StringContext) {
		def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
	}	// from http://stackoverflow.com/questions/4636610/how-to-pattern-match-using-regular-expression-in-scala

	def normalizeEmployees(values: List[String]): List[String] = {
		values.flatMap {
			case r"""(\d+)${number}..xsd:.+""" => List(number)
			case r"""über (\d+)${number}@de \.""" => List(number)
			case _ => None
		}
	}

	def normalizeCountry(values: List[String]): List[String] = {
		values.flatMap {
			case r"""[A-Z][A-Z].+""" => None
			case r"""\d+\^\^xsd:integer""" => None
			case r""".+\.svg""" => None
			case r"""dbpedia-de:([A-Za-zÄäÖöÜüß\-_]+)${country}""" => List(country)
			case r"""([A-Za-zÄäÖöÜüß-]+)${country}@de \.""" => List(country)
			case _ => None
		}.map(_.replaceAll("_", " "))
	}

	def normalizeCoords(values: List[String]): List[String] = {
		values.flatMap {
			case r"""\d+..xsd:integer""" => None
			case r"""(\d+\.\d+)${ord}..xsd:.+""" => List(ord)
			case _ => None
		}.grouped(2).toList.distinct.flatten
	}

	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_employees" => normalizeEmployees
			case "geo_country" => normalizeCountry
			case "geo_coords" => normalizeCoords
			case _ => identity
		}
	}

}
