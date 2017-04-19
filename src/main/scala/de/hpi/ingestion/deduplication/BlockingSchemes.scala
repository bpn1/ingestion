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
  * This class uses the result of the CompSplit.jar on the name property as key.
  */
class DefaultBlockingScheme extends BlockingScheme {
	/*override def generateKey(subject: Subject): List[String] = {
		val fileStream = getClass.getResource("/bin/StanfordCRFClassifier-Tag.bin").openStream()
		val classifier = In.stream(fileStream).readObject[AClassifier[Tag]]()
		val taggedName = classifier.getTags(subject.get("name").head)
		val tagsToCheck = List(Tag.LEGAL_FORM, Tag.PERSON_LAST_NAME, Tag.SECTOR)

		var key: List[String] = List()
		for (tuple <- taggedName.sortBy(_.getRight)) {
			if (tagsToCheck.contains(tuple.getRight)) {
				key ++= List(tuple.toString())
			}
		}
		key
	}*/
	override def generateKey(subject: Subject): List[String] = List("lol")
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
