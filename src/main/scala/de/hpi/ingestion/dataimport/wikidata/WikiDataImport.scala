package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable.ListBuffer

object WikiDataImport {
	val defaultInputFile = "wikidata.json"
	val language = "de"
	val fallbackLanguage = "en"
	val keyspace = "wikidumps"
	val tablename = "wikidata"

	/**
	  * Removes array syntax from Json for parallel parsing of the JSON objects.
	  * @param json JSON String to clean
	  * @return cleaned JSON String in which each line is either a JSON object or empty
	  */
	def cleanJSON(json: String): String = {
		json.replaceAll("^\\[|,$|, $|\\]$", "")
	}

	// TODO: use *parameter
	def getValue(json: JsValue, path: List[String]): Option[JsValue] = {
		var element = json
		for(pathSegment <- path) {
			if(element.as[JsObject].value.contains(pathSegment)) {
				element = (element \ pathSegment).as[JsValue]
			} else {
				return None
			}
		}
		if(element.getClass == classOf[JsUndefined]) {
			None
		} else {
			Option(element)
		}
	}

	def extractString(json: JsValue, path: List[String]): Option[String] = {
		val value = getValue(json, path)
		value.map(_.as[String])
	}

	def extractList(json: JsValue, path: List[String]): List[JsValue] = {
		val value = getValue(json, path).map(_.as[List[JsValue]])
		value.getOrElse(List[JsValue]())
	}

	def extractMap(json: JsValue, path: List[String]): Map[String, JsValue] = {
		val value = getValue(json, path).map(_.as[Map[String, JsValue]])
		value.getOrElse(Map[String, JsValue]())
	}

	def extractDouble(json: JsValue, path: List[String]): Option[String] = {
		val value = getValue(json, path)
		value.map(_.as[Double].toString)
	}

	/**
	  * Extracts data of given data type out of JSON object.
	  * @param dataType data type to extract
	  * @param dataValue JSON object to extract the data from
	  * @return value of the data type as String
	  */
	def parseDataType(dataType: Option[String], dataValue: JsValue): String = {
		val propertyPathMap = Map(
			"string" -> List("value"),
			"wikibase-entityid" -> List("value", "id"),
			"time" -> List("value", "time"),
			"monolingualtext" -> List("value", "text"))

		val propertyKey = dataType.getOrElse("")
		if(propertyPathMap.contains(propertyKey)) {
			extractString(dataValue, propertyPathMap(propertyKey)).getOrElse("")
		} else {
			propertyKey match {
				case "globecoordinate" =>
					val lat = extractDouble(dataValue, List("value", "latitude")).getOrElse("")
					val long = extractDouble(dataValue, List("value", "longitude")).getOrElse("")
					lat + ";" + long
				case "quantity" =>
					val amount = extractString(dataValue, List("value", "amount")).getOrElse("")
					val unit = extractString(dataValue, List("value", "unit")).getOrElse("")
						.split("/")
						.last
					amount + ";" + unit
				case _ => ""
			}
		}
	}

	def fillEntityValues(json: JsValue): WikiDataEntity = {
		val entity = WikiDataEntity(extractString(json, List("id")).getOrElse(""))
		entity.entitytype = extractString(json, List("type"))
		entity.wikiname = extractString(json, List("sitelinks", language + "wiki", "title"))
		entity.description = extractString(json, List("descriptions", language, "value"))
			.orElse(extractString(json, List("descriptions", fallbackLanguage, "value")))

		// fall back to secondary language or first available language for label
		val labels = getValue(json, List("labels"))
		if(labels.isDefined) {
			val labelLanguages = labels.get.as[JsObject].keys
			if(labelLanguages.contains(language) && !entity.entitytype.contains("property")) {
				entity.label = extractString(labels.get, List(language, "value"))
			} else if(labelLanguages.contains(fallbackLanguage)) {
				entity.label = extractString(labels.get, List(fallbackLanguage, "value"))
			} else if(labelLanguages.nonEmpty) {
				// take first available language if other two languages don't exist
				val firstLanguage = labelLanguages.headOption.getOrElse("")
				entity.label = extractString(labels.get, List(firstLanguage, "value"))
			}
		}

		entity
	}

	def extractClaimValues(claim: JsValue): String = {
		val dataValue = getValue(claim, List("mainsnak", "datavalue"))
		var value = ""
		if(dataValue.isDefined) {
			val dataType = extractString(dataValue.get, List("type"))
			value = parseDataType(dataType, dataValue.get)
		}
		value
	}

	def parseEntity(line: String): WikiDataEntity = {
		val json = Json.parse(line)
		val entity = fillEntityValues(json)

		// concatenate aliases in all languages
		val aliases = getValue(json, List("aliases"))
			.getOrElse(JsObject(Seq[(String, JsValue)]()))
			.as[JsObject]
		val aliasBuffer = ListBuffer[String]()
		for(language <- aliases.keys) {
			aliasBuffer ++= extractList(aliases, List(language))
				.map(jsVal => extractString(jsVal, List("value")).getOrElse(""))
		}
		entity.aliases = aliasBuffer.toList

		val claims = extractMap(json, List("claims"))
		entity.data = claims.mapValues { content =>
			content.as[List[JsValue]]
				.map(extractClaimValues)
				.filter(_.nonEmpty)
		}
		entity
	}

	/**
	  * Translates Wikidata id of property to their label. If there is no label for the property,
	  * the id will be kept.
	  * @param entity Wikidata Entity whose properties will be translated
	  * @param properties Map of property id pointing to its label
	  * @return Wikidata Entity with translated property ids
	  */
	def translatePropertyIDs(
		entity: WikiDataEntity,
		properties: Map[String, String]
	): WikiDataEntity = {
		entity.data = entity.data.map { case (key, value) =>
			(properties.getOrElse(key, key), value)
		}
		entity
	}

	/**
	  * Builds property map containing each Wikidata property and its label.
	  * @param wikidataEntities RDD of Wikidata Entities including the property entities
	  * @return Map of property id pointing to its label
	  */
	def buildPropertyMap(wikidataEntities: RDD[WikiDataEntity]): Map[String, String] = {
		val propertyType = "property"
		wikidataEntities
			.filter(entity => entity.label.isDefined && entity.entitytype.contains(propertyType))
			.map(entity => (entity.id, entity.label.get))
			.collect
			.toMap
	}

	def main(args: Array[String]) {
		var inputFile = defaultInputFile
		if(args.length > 0) {
			inputFile = args(0)
		}

		val conf = new SparkConf()
			.setAppName("WikiDataImport")

		val sc = new SparkContext(conf)
		val jsonFile = sc.textFile(inputFile)

		val wikiData = jsonFile
			.map(cleanJSON)
			.filter(_.nonEmpty)
			.map(parseEntity)

		val properties = buildPropertyMap(wikiData)
		val propertyBroadcast = sc.broadcast(properties)

		wikiData.mapPartitions({ partition =>
			val localPropertyMap = propertyBroadcast.value
			partition.map(translatePropertyIDs(_, localPropertyMap))
		}, true)
			.saveToCassandra(keyspace, tablename)
	}
}
