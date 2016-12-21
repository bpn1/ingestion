import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable
import play.api.libs.json._
import com.datastax.spark.connector._

object WikiDataRDD {
	val defaultInputFile = "wikidata.json"
	val language = "de"
	val fallbackLanguage = "en"

	case class WikiDataEntity(var id: String = null, var entitytype: String = null,
		var wikiname: String = null, var description: String = null, var label: String = null,
		var aliases: List[String] = null, var data: Map[String, List[String]] = Map[String, List[String]]())

	// remove array syntax from JSON for parallel parsing
	def cleanJSON(json: String): String = {
		json.replaceAll("^\\[|,$|, $|\\]$", "")
	}

	def getValue(json: JsValue, path: List[String]): JsValue = {
		var element = json
		try {
			for(pathSegment <- path) {
				element = (element \ pathSegment).as[JsValue]
			}
		} catch {
			case jre: JsResultException =>
				return null
			case npe: NullPointerException =>
				return null
		}

		if(element.getClass == classOf[JsUndefined])
			return null

		element
	}

	def extractString(json: JsValue, path: List[String]): String = {
		val value = getValue(json, path)
		try {
			if(value != null)
				value.as[String]
			else
				null
		} catch {
			case e : Exception =>
				println(value + " " + value.getClass)
				null
		}

	}

	def extractList(json: JsValue, path: List[String]): List[JsValue] = {
		val value = getValue(json, path)
		if(value != null)
			value.as[List[JsValue]]
		else
			null
	}

	def extractMap(json: JsValue, path: List[String]): Map[String, JsValue] = {
		val value = getValue(json, path)
		if(value != null)
			value.as[Map[String, JsValue]]
		else
			null
	}

	def extractDouble(json: JsValue, path: List[String]): String = {
		val value = getValue(json, path)
		if(value != null)
			value.as[Double].toString
		else
			null
	}

	def parseDataType(dataType: String, dataValue: JsValue): String = {
		dataType match {
			case "string" =>
				extractString(dataValue, List("value"))
			case "wikibase-entityid" =>
				extractString(dataValue, List("value", "id"))
			case "time" =>
				extractString(dataValue, List("value", "time"))
			case "globecoordinate" =>
				extractDouble(dataValue, List("value", "latitude")) + ";" + extractDouble(dataValue, List("value", "longitude"))
			case "quantity" =>
				extractString(dataValue, List("value", "amount")) + ";" + extractString(dataValue, List("value", "unit")).split("/").last
			case "monolingualtext" =>
				extractString(dataValue, List("value", "text"))
			case null =>
				""
			case _ =>
				"DataType " + dataType + " not supported"
		}
	}

	def parseEntity(line: String): WikiDataEntity = {
		val json = Json.parse(line)
		val entity = WikiDataEntity()

		entity.id = extractString(json, List("id"))
		entity.entitytype = extractString(json, List("type"))
		entity.wikiname = extractString(json, List("sitelinks", language + "wiki", "title"))

		entity.description = extractString(json, List("descriptions", language, "value"))
		if(entity.description == null) {
			entity.description = extractString(json, List("descriptions", fallbackLanguage, "value"))
		}

		// fall back to secondary language or first available language for label
		val labels = getValue(json, List("labels")).as[JsObject]
		val labelLanguages = labels.keys

		if(labelLanguages.contains(language) && entity.entitytype != "property") {
			entity.label = extractString(labels, List(language, "value"))
		} else if(labelLanguages.contains(fallbackLanguage)) {
			entity.label = extractString(labels, List(fallbackLanguage, "value"))
		} else if(labelLanguages.size > 0) {
			// take first available language if other two languages don't exist
			val firstLanguage = labelLanguages.head
			entity.label = extractString(labels, List(firstLanguage, "value"))
		}

		// concatenate aliases in all languages
		val aliases = getValue(json, List("aliases")).as[JsObject]
		val aliasBuffer = mutable.ListBuffer[String]()

		for(language <- aliases.keys) {
			try {
				aliasBuffer ++= extractList(aliases, List(language))
					.map(jsVal => extractString(jsVal, List("value")))
			} catch {
				case e: JsResultException =>
					println(aliases, aliases.getClass)
			}
		}

		entity.aliases = aliasBuffer.toList

		val claims = extractMap(json, List("claims"))
		val claimBuffer = mutable.ListBuffer[(String, List[String])]()
		if(claims != null) {
			for((property, content) <- claims) {
				val propertyBuffer = mutable.ListBuffer[String]()
				for(claim <- content.as[List[JsValue]]) {
					val dataValue = getValue(claim, List("mainsnak", "datavalue"))
					val dataType = extractString(dataValue, List("type"))
					val value = parseDataType(dataType, dataValue)
					if(value != "")
						propertyBuffer += value
				}
				claimBuffer += Tuple2(property, propertyBuffer.toList)
			}
		}
		entity.data = claimBuffer.toMap

		entity
	}

	def translatePropertyIDs(entity: WikiDataEntity, properties: Map[String, String]): WikiDataEntity = {
		val translatedData = entity.data.map { case (key, value) =>
			val newKey = properties.getOrElse(key, key) // keep original key if there is no such key in the Map
			(newKey, value)
		}.toMap

		entity.data = translatedData
		entity
	}

	def main(args: Array[String]) {
		var inputFile = defaultInputFile
		if(args.length > 0)
			inputFile = args(0)

		val conf = new SparkConf()
			.setAppName("WikiDataRDD")
			.set("spark.cassandra.connection.host", "172.20.21.11")

		val sc = new SparkContext(conf)
		val jsonFile = sc.textFile(inputFile)
		val wikiData = jsonFile
			.map(cleanJSON)
			.filter(_ != "")
			.map(parseEntity)
			//.cache

		val properties = wikiData
			.filter(entity => entity.entitytype == "property" && entity.label != null)
			.map(entity => (entity.id, entity.label))
			.collect
			.toMap

		var propertyBroadcast = sc.broadcast(properties)

		wikiData
			.map(translatePropertyIDs(_, propertyBroadcast.value))
			.saveToCassandra("wikidumps", "wikidata", SomeColumns("id", "entitytype", "wikiname", "description", "label", "aliases", "data"))
	}
}
