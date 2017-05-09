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
		def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
	}	// from http://stackoverflow.com/questions/4636610/how-to-pattern-match-using-regular-expression-in-scala
}
