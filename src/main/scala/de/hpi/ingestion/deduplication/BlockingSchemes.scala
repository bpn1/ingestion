package de.hpi.ingestion.deduplication

import de.hpi.companies.algo.classifier.AClassifier
import de.hpi.companies.algo.Tag
import com.github.powerlibraries.io.In
import de.hpi.ingestion.datalake.models.Subject
import scala.collection.JavaConversions._

/**
  * This trait is the template for our blocking schemes.
  */
trait BlockingScheme extends Serializable {
	val undefinedValue = List("undefined")
	protected var inputAttributes: List[String] = List[String]()

	/**
	  * Gets the attributes to generate the key with as input and converts them to a list
	  * if required for the blocking scheme.
	  * @param attrList Sequence of attributes to use.
	  */
	def setAttributes(attrList: String*): Unit = inputAttributes = attrList.toList

	/**
	  * Generates key from the subject's properties.
	  * @param subject Subject to use.
	  * @return Key as list of strings.
	  */
	def generateKey(subject: Subject): List[String]
}

/**
  * This class uses the name property as key. For every word in the name, it creates a pair of the
  * word and a fitting tag and filters these after the tags to check.
  */
class DefaultBlockingScheme extends BlockingScheme {
	override def generateKey(subject: Subject): List[String] = {
		subject.name.map { name =>
			val fileStream = getClass.getResource("/bin/StanfordCRFClassifier-Tag.bin").openStream()
			val classifier = In.stream(fileStream).readObject[AClassifier[Tag]]()
			val taggedName = classifier.getTags(name)
			val tagsToCheck = List(Tag.LEGAL_FORM, Tag.PERSON_LAST_NAME, Tag.SECTOR)

			taggedName
				.filter(tuple => tagsToCheck.contains(tuple.getRight))
				.sortBy(_.getRight)
				.map(_.toString)
				.toList

			}.filter(_.nonEmpty)
		.getOrElse(undefinedValue)
	}
}

/**
  * This class uses a list of input attributes as key.
  */
class ListBlockingScheme extends BlockingScheme {
	override def generateKey(subject: Subject): List[String] = {
		val key = inputAttributes.flatMap(subject.get)
		if (key.isEmpty) {
			undefinedValue
		} else {
			key
		}
	}
}
