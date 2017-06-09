package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import play.api.libs.json._

/**
  * This job parses a Wikidata JSON dump into Wikidata entities and imports them into the Cassandra.
  */
object WikiDataImport extends SparkJob {
	appName = "WikiDataImport"
	val defaultInputFile = "wikidata.json"
	val language = "de"
	val fallbackLanguage = "en"
	val keyspace = "wikidumps"
	val tablename = "wikidata"

	// $COVERAGE-OFF$
	/**
	  * Reads the Wikidata JSON dump from the HDFS.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val inputFile = if(args.length > 0) args.head else defaultInputFile
		List(sc.textFile(inputFile)).toAnyRDD()
	}

	/**
	  * Saves the parsed Wikidata Entities to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[WikiDataEntity]()
			.head
			.saveToCassandra(keyspace, tablename)
	}
	// $COVERAGE-ON$

	/**
	  * Removes array syntax from Json for parallel parsing of the JSON objects.
	  * @param json JSON String to clean
	  * @return cleaned JSON String in which each line is either a JSON object or empty
	  */
	def cleanJSON(json: String): String = {
		json.replaceAll("^\\[|,$|, $|\\]$", "")
	}

	/**
	  * Extracts a JSON value from a JSON object.
	  * @param json JSON object containing the data
	  * @param path JSON path of the object fields to traverse
	  * @return JSON value of the JSON field
	  */
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

	/**
	  * Extracts a String value from a JSON object.
	  * @param json JSON object containing the data
	  * @param path JSON path of the object fields to traverse
	  * @return String value of the JSON field
	  */
	def extractString(json: JsValue, path: List[String]): Option[String] = {
		val value = getValue(json, path)
		value.map(_.as[String])
	}

	/**
	  * Extracts a JSON array as List from a JSON object.
	  * @param json JSON object containing the data
	  * @param path JSON path of the object fields to traverse
	  * @return JSON array as List
	  */
	def extractList(json: JsValue, path: List[String]): List[JsValue] = {
		val value = getValue(json, path).map(_.as[List[JsValue]])
		value.toList.flatten
	}

	/**
	  * Extracts a JSON object as Map from a JSON object.
	  * @param json JSON object containing the data
	  * @param path JSON path of the object fields to traverse
	  * @return JSON object as Map
	  */
	def extractMap(json: JsValue, path: List[String]): Map[String, JsValue] = {
		val value = getValue(json, path).map(_.as[Map[String, JsValue]])
		value.toList.flatten.toMap
	}

	/**
	  * Extracts a Double value from a JSON object.
	  * @param json JSON object containing the data
	  * @param path JSON path of the object fields to traverse
	  * @return Double value of the JSON field as String
	  */
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
	def parseDataType(dataType: Option[String], dataValue: JsValue): Option[String] = {
		val propertyPathMap = Map(
			"string" -> List("value"),
			"wikibase-entityid" -> List("value", "id"),
			"time" -> List("value", "time"),
			"monolingualtext" -> List("value", "text"))

		dataType
			.flatMap(propertyPathMap.get)
			.flatMap(extractString(dataValue, _))
			.orElse(dataType.collect {
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
			})
	}

	/**
	  * Extracts the label for a WikiDataEntity from a given JSON object. The first choice of the label langauge is
	  * {@language} if the entity is not a property. The second choice of the language is {@fallbackLanguage}. If both
	  * of these are not available the first available language is taken.
	  * @param json JSON object containing the data
	  * @param entityType type of the entity for which the labels are extracted
	  * @return Option of the label (if it exists
	  */
	def extractLabels(json: JsValue, entityType: Option[String]): Option[String] = {
		val labelMap = extractMap(json, List("labels"))
		labelMap.get(language)
			.filter(lang => !entityType.contains("property"))
			.orElse(labelMap.get(fallbackLanguage))
			.orElse(labelMap.values.headOption)
			.flatMap(extractString(_, List("value")))
	}

	/**
	  * Extracts every field other than the properties and aliases from the json object into a WikiDataEntity.
	  * @param json JSON object of the Wikidata entry
	  * @return WikiDataEntity with every field other than the properties and aliases filled with data
	  */
	def fillEntityValues(json: JsValue): WikiDataEntity = {
		val entity = WikiDataEntity(extractString(json, List("id")).getOrElse(""))
		entity.entitytype = extractString(json, List("type"))
		entity.wikiname = extractString(json, List("sitelinks", language + "wiki", "title"))
		entity.description = extractString(json, List("descriptions", language, "value"))
			.orElse(extractString(json, List("descriptions", fallbackLanguage, "value")))
		entity.label = extractLabels(json, entity.entitytype)
		entity
	}

	/**
	  * Extracts the value of a given claim in JSON and returns it as a String
	  * @param claim JSON object of the claim
	  * @return value of the claim as String
	  */
	def extractClaimValues(claim: JsValue): Option[String] = {
		val dataValue = getValue(claim, List("mainsnak", "datavalue"))
		dataValue.flatMap { value =>
			val dataType = extractString(value, List("type"))
			parseDataType(dataType, value)
		}
	}

	/**
	  * Extracts the aliases of a WikiDataEntity out of a given JSON object
	  * @param json JSON object of the Wikidata entry containing the data
	  * @return List of all aliases contained in the JSON object
	  */
	def extractAliases(json: JsValue): List[String] = {
		val aliasJsonObject = getValue(json, List("aliases"))
			.map(_.as[JsObject])
		val languageList = aliasJsonObject
			.map(_.keys.toList)
		    .getOrElse(Nil)

		languageList.flatMap { language =>
			extractList(aliasJsonObject.get, List(language))
				.flatMap(extractString(_, List("value")))
		}
	}

	/**
	  * Parses a JSON object given as String into a WikiDataEntity
	  * @param line String containing the JSON object
	  * @return WikiDataEntity filled with the data contained in the JSON object
	  */
	def parseEntity(line: String): WikiDataEntity = {
		val json = Json.parse(line)
		val entity = fillEntityValues(json)
		entity.aliases = extractAliases(json)

		val claims = extractMap(json, List("claims"))
		entity.data = claims.mapValues { content =>
			content
				.as[List[JsValue]]
				.flatMap(extractClaimValues)
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
	def translatePropertyIDs(entity: WikiDataEntity, properties: Map[String, String]): WikiDataEntity = {
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

	/**
	  * Parses the JSON Wikidata dump into Wikidata Entities.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val jsonFile = input.fromAnyRDD[String]().head
		val wikiData = jsonFile
			.map(cleanJSON)
			.filter(_.nonEmpty)
			.map(parseEntity)
		val properties = buildPropertyMap(wikiData)
		val propertyBroadcast = sc.broadcast(properties)

		val resolvedWikiData = wikiData.mapPartitions({ partition =>
			val localPropertyMap = propertyBroadcast.value
			partition.map(translatePropertyIDs(_, localPropertyMap))
		}, true)
		List(resolvedWikiData).toAnyRDD()
	}
}
