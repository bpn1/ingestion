package DataLake

/**
  * This trait is the template for our blocking schemes.
  */
trait BlockingScheme extends Serializable {
	var inputAttributes = List[String]()

	/**
	  * Generates key from the subject's properties.
	  * @param subject Subject to use.
	  * @return Key as list of strings.
	  */
	def generateKey(subject: Subject): List[String]
}

/**
  * This class uses the first three characters of the name property as key.
  */
object SimpleBlockingScheme extends BlockingScheme {
	override def generateKey(subject: Subject): List[String] = {
		val name = subject.name.getOrElse("")
		List(name.substring(0, Math.min(3, name.length)))
	}
}

/**
	* This class uses the first two characters of the input attributes as key.
	*/
object ListBlockingScheme extends BlockingScheme {
	override def generateKey(subject: Subject): List[String] = {
		val attributeList = inputAttributes.flatMap(subject.get)
		List(attributeList.map(
			attribute => attribute.substring(0, Math.min(2, attribute.length)))
			.mkString(""))
	}
}
