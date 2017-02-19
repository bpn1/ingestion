import org.apache.spark.SparkContext
import scala.collection.mutable
import java.util.UUID

class SubjectManager(subject: Subject, templateVersion: Version) {
	def makeVersion(value: List[String], validity: Map[String, String] = null): Version = {
		templateVersion.copy(value = value, validity = validity)
	}

	def setName(value: String, validity: Map[String, String] = null) {
		subject.name = Option(value)
		subject.name_history = List(makeVersion(List(value), validity))
	}

	def addAliases(value: List[String], validity: Map[String, String] = null) {
		subject.aliases = value ++ subject.aliases
		subject.aliases_history = List(makeVersion(subject.aliases, validity))
	}

	def setCategory(value: String, validity: Map[String, String] = null) {
		subject.category = Option(value)
		subject.category_history = List(makeVersion(List(value), validity))
	}

	def addProperties(value: Map[String, List[String]], validityMap: Map[String, Map[String, String]] = null) {
		val buffer = mutable.Map[String, List[String]]()
		buffer ++= subject.properties
		val historyBuffer = mutable.Map[String, List[Version]]()
		historyBuffer ++= subject.properties_history

		// add a history entry and do deduplication for every field
		for((key, list) <- value) {
			val oldList = buffer.getOrElseUpdate(key, List[String]())
			// check if the lists contain the same elements
			if(oldList.toSet != list.toSet) {
				buffer(key) = (oldList ++ list).distinct
				val oldHistory = historyBuffer.getOrElseUpdate(key, List[Version]())

				var validity: Map[String, String] = null
				if(validityMap != null)
					validity = validityMap.getOrElse(key, null)
				historyBuffer(key) = oldHistory ++ List(makeVersion(buffer(key), validity))
			}
		}
		
		subject.properties = buffer.toMap
		subject.properties_history = historyBuffer.toMap
	}

	def addRelations(relations: Map[UUID, Map[String, String]], validityMap: Map[UUID, Map[String, Map[String, String]]] = null) {
		val buffer = mutable.Map[UUID, Map[String, String]]()
		buffer ++= subject.relations

		val historyBuffer = mutable.Map[UUID, Map[String, List[Version]]]()
		historyBuffer ++= subject.relations_history

		// add a history entry for every field
		for((targetID, properties) <- relations) {
			val valueBuffer = mutable.Map[String, String]()
			val versionBuffer = mutable.Map[String, List[Version]]()

			for((key, value) <- properties) {
				val oldValue = buffer.getOrElseUpdate(targetID, Map[String, String]()).getOrElse(key, null)

				if(value != oldValue) {
					val oldHistory = historyBuffer.getOrElse(targetID, mutable.Map[String, List[Version]]()).getOrElse(key, List[Version]())
					valueBuffer(key) = value

					var validity: Map[String, String] = null
					if(validityMap != null)
						validity = validityMap.getOrElse(targetID, Map[String, Map[String, String]]()).getOrElse(key, null)
					versionBuffer(key) = oldHistory ++ List(makeVersion(List(value), validity))
				}
			}
			
			buffer(targetID) = valueBuffer.toMap
			historyBuffer(targetID) = versionBuffer.toMap
		}
		
		subject.relations = buffer.toMap
		subject.relations_history = historyBuffer.toMap
	}
}