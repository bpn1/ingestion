package de.hpi.ingestion.implicits

/**
  * Contains implicit classes extending Scala Regex.
  */
object RegexImplicits {
	/**
	  * Implicit class for using regex in pattern matching
	  * @param sc String Context
	  */
	implicit class Regex(sc: StringContext) {
		/**
		  * http://stackoverflow.com/a/16256935/6625021
		  * @return matcher
		  */
		def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
	}
}
