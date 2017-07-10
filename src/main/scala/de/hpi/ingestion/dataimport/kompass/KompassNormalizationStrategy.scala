package de.hpi.ingestion.dataimport.kompass

import de.hpi.ingestion.implicits.RegexImplicits._
import de.hpi.ingestion.dataimport.SharedNormalizations

object KompassNormalizationStrategy extends Serializable {
	def normalizeTurnover(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([0-9\.]+)${value} EUR""" => List(value)
			case r"""([0-9]+)${value} Million[en]{0,2} EUR""" => List(value + "000000")
			case r"""< ([0-9 ]+)${value} EUR""" => List("0", value)
			case r"""([0-9]+)${first} - ([0-9]+)${second} Millionen EUR""" => List(first, second).map(_ + "000000")
			case r"""([0-9 ]+)${first} - ([0-9]+)${second} Million[en]{0,2} EUR""" => List(first, second + "000000")
			case _ => Nil
		}.map(_.replaceAll("( |\\.)", "")).distinct
	}

	def normalizeCapital(values: List[String]): List[String] = {
		values.flatMap {
			case r"""([0-9\.]+)${value} EUR""" => List(value)
			case _ => Nil
		}.map(_.replaceAll("( |\\.)", "")).distinct
	}

	def normalizeEmployees(values: List[String]): List[String] = {
		values.flatMap {
			case r"""Von ([0-9]+)${first} bis ([0-9]+)${second} Beschäftigte""" => List(first, second)
			case r"""([0-9]+)${value} Beschäftigte""" => List(value)
			case r"""Mehr als ([0-9]+)${value} Beschäftigte""" => List(value)
			case _ => Nil
		}.map(_.replaceAll("( |\\.)", "")).distinct
	}

	def apply(attribute: String): (List[String]) => List[String] = {
		attribute match {
			case "gen_legal_form" => SharedNormalizations.normalizeLegalForm
			case "gen_capital" => normalizeCapital
			case "gen_turnover" => normalizeTurnover
			case "gen_employees" => normalizeEmployees
			case _ => _
		}
	}

}
