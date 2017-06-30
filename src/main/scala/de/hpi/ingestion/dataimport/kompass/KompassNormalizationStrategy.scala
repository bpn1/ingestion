package de.hpi.ingestion.dataimport.kompass

import de.hpi.ingestion.implicits.RegexImplicits._
import de.hpi.ingestion.dataimport.SharedNormalizations

object KompassNormalizationStrategy extends Serializable {
	/**
	  * Normalizes all other values by default (removing pre- and suffixes and dashes)
	  * @param values Strings to be normalized
	  * @return normalized strings
	  */
	def normalizeDefault(values: List[String]): List[String] = {
		values.flatMap {
			case r"""(.+)${value}\^\^xsd:.+""" => List(value)
			case r"""(.+)${value}@de \.""" => List(value)
			case r"""dbpedia-de:(.+)${value}""" => List(value)
			case other => List(other)
		}.map(_.replaceAll("(_|-)", " ")).distinct
	}

	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_legal_form" => SharedNormalizations.normalizeLegalForm	//ToDo: checken, ob das reicht
			//ToDo: case "id_umsatzsteuer"
			//ToDo: case "gen_capital" //75.000 EUR
			//ToDo: case "gen_turnover" //1 - 2 Millionen EUR //> 500 Millionen EUR
			//ToDo: case "gen_employees" //Von 10 bis 19 Beschäftigte //100 Beschäftigte
			//ToDo: case "gen_street" //Herriotstr. 3 //aktuell bei addressextraction gemacht
			case _ => this.normalizeDefault
		}
	}

}
