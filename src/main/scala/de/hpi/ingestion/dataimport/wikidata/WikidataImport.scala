/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.dataimport.wikidata

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.JSONEntityParser
import de.hpi.ingestion.dataimport.wikidata.models.WikidataEntity
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import play.api.libs.json._

/**
  * Parses each article from the Wikidata JSON dump into a `WikidataEntity` and imports it into the Cassandra.
  */
class WikidataImport extends SparkJob with JSONEntityParser[WikidataEntity] {
    import WikidataImport._
    appName = "Wikidata Import"
    configFile = "wikidata_import.xml"

    var wikidataDump: RDD[String] = _
    var wikidataEntities: RDD[WikidataEntity] = _
    // $COVERAGE-OFF$
    /**
      * Reads the Wikidata JSON dump from the HDFS.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        wikidataDump = sc.textFile(settings("inputFile"))
    }

    /**
      * Saves the parsed Wikidata Entities to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        wikidataEntities.saveToCassandra(settings("keyspace"), settings("wikidataTable"))
    }
    // $COVERAGE-ON$

    /**
      * Parses a JSON object into a WikidataEntity
      * @param json JSON-Object containing the data
      * @return WikidataEntity containing the parsed data
      */
    override def fillEntityValues(json: JsValue): WikidataEntity = {
        val entity = fillSimpleValues(json)
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
      * Extracts every field other than the properties and aliases from the json object into a WikidataEntity.
      * @param json JSON object of the Wikidata entry
      * @return WikidataEntity with every field other than the properties and aliases filled with data
      */
    def fillSimpleValues(json: JsValue): WikidataEntity = {
        val entity = WikidataEntity(extractString(json, List("id")).getOrElse(""))
        entity.entitytype = extractString(json, List("type"))
        entity.wikiname = extractString(json, List("sitelinks", settings("language") + "wiki", "title"))
        entity.description = extractString(json, List("descriptions", settings("language"), "value"))
            .orElse(extractString(json, List("descriptions", settings("fallbackLanguage"), "value")))
        entity.label = extractLabels(json, entity.entitytype)
        entity
    }

    /**
      * Extracts the label for a WikidataEntity from a given JSON object. The first choice of the label language
      * (defined in the config) is used if the entity is not a property. The second choice of the language is defined in
      * the config as well. If both of these are not available then the first available language is taken.
      * @param json JSON object containing the data
      * @param entityType type of the entity for which the labels are extracted
      * @return Option of the label (if it exists
      */
    def extractLabels(json: JsValue, entityType: Option[String]): Option[String] = {
        val labelMap = extractMap(json, List("labels"))
        labelMap.get(settings("language"))
            .filter(lang => !entityType.contains("property"))
            .orElse(labelMap.get(settings("fallbackLanguage")))
            .orElse(labelMap.values.headOption)
            .flatMap(extractString(_, List("value")))
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
      * Extracts the aliases of a WikidataEntity out of a given JSON object
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
      * Parses each article from the Wikidata JSON dump into a WikidataEntity.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val wikiData = wikidataDump
            .map(cleanJSON)
            .collect { case line: String if line.nonEmpty => parseJSON(line) }
        val properties = buildPropertyMap(wikiData)
        val propertyBroadcast = sc.broadcast(properties)

        wikidataEntities = wikiData.mapPartitions({ partition =>
            val localPropertyMap = propertyBroadcast.value
            partition.map(translatePropertyIDs(_, localPropertyMap))
        }, true)
    }
}

object WikidataImport {
    /**
      * Translates Wikidata id of property to their label. If there is no label for the property,
      * the id will be kept.
      * @param entity Wikidata Entity whose properties will be translated
      * @param properties Map of property id pointing to its label
      * @return Wikidata Entity with translated property ids
      */
    def translatePropertyIDs(entity: WikidataEntity, properties: Map[String, String]): WikidataEntity = {
        entity.copy(data = entity.data.mapKeys(key => properties.getOrElse(key, key)))
    }

    /**
      * Builds property map containing each Wikidata property and its label.
      * @param wikidataEntities RDD of Wikidata Entities including the property entities
      * @return Map of property id pointing to its label
      */
    def buildPropertyMap(wikidataEntities: RDD[WikidataEntity]): Map[String, String] = {
        val propertyType = "property"
        wikidataEntities
            .filter(entity => entity.label.isDefined && entity.entitytype.contains(propertyType))
            .map(entity => (entity.id, entity.label.get))
            .collect
            .toMap
    }
}
