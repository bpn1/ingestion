package de.hpi.ingestion.implicits

/**
  * Contains implicit classes extending Scala Chars.
  */
object CharImplicits {

	/**
	  * Implicit class which adds the check if a Char is an ASCII Char.
	  * From http://stackoverflow.com/a/15442719
	  * @param ch character on which the implicit methods are called.
	  */
	implicit class CharProperties(val ch: Char) extends AnyVal {
		/**
		  * Checks if the Char this method is called on is an ASCII character.
		  * @return true if the Char is an ASCII character
		  */
		def isASCIILetter: Boolean = ch.isLetter && ch <= 'z'
	}
}
