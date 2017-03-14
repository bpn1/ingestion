package DataLake

/**
	* This trait is the template for our blocking schemes.
	*/
trait BlockingScheme extends Serializable {

	/**
		* Generates key from the subject's properties.
		* @param subject Subject to use.
		* @return Key as String.
		*/
	def generateKey(subject: Subject): String
}

/**
	* This class uses the first three characters of the name property as key.
	*/
object SimpleBlockingScheme extends BlockingScheme {
	override def generateKey(subject: Subject) = {
		val name = subject.name.getOrElse("")
		name.substring(0, Math.min(3, name.length))
	}
}
