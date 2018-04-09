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

package de.hpi.ingestion.dataimport

import java.text.SimpleDateFormat
import java.util.Date

import play.api.libs.json.{JsArray, JsObject, JsString, JsValue}

/**
  * Trait to parse JSON-Objects. Contains all methods needed to parse JSON into Scala Objects.
  */
trait JSONParser {
    /**
      * Extracts a JSON value from a JSON object.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return JSON value of the JSON field
      */
    def getValue(json: JsValue, path: List[String]): Option[JsValue] = {
        path.foldLeft(Option(json)) { (jsonOption, pathSegment) =>
            jsonOption
                .filter(_.isInstanceOf[JsObject])
                .filter(_.as[JsObject].keys.contains(pathSegment))
                .map(_ \ pathSegment)
                .map(_.as[JsValue])
        }
    }

    /**
      * Extracts a JSON array as List from a JSON object.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return JSON array as List
      */
    def extractList(json: JsValue, path: List[String]): List[JsValue] = {
        getValue(json, path)
            .map(_.as[List[JsValue]])
            .toList
            .flatten
    }

    /**
      * Extracts a JSON array as List from a JSON object and converts it to a List of Strings.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return JSON array as List of Strings
      */
    def extractStringList(json: JsValue, path: List[String]): List[String] = {
        getValue(json, path)
            .map(_.as[List[String]])
            .toList
            .flatten
    }

    /**
      * Extracts a JSON object as Map from a JSON object.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return JSON object as Map
      */
    def extractMap(json: JsValue, path: List[String]): Map[String, JsValue] = {
        getValue(json, path)
            .map(_.as[Map[String, JsValue]])
            .toList
            .flatten
            .toMap
    }

    /**
      * Extracts nested JSON-Objects as a List of String-Tuples (representing the key value pairs) .
      * @param nestedTuple Tuple of the key of the JSON-Object and the JSON-Object itself
      * @return List of key value pairs as Tuples of Strings
      */
    def flattenNestedMap(nestedTuple: (String, JsValue)): List[(String, String)] = {
        val (key, jsonObject) = nestedTuple
        jsonObject match {
            case t: JsObject => t.as[Map[String, String]]
                .map { case (nestedKey, nestedValue) => (s"$key.$nestedKey", nestedValue) }.toList
            // TODO: #276 JsArray parsing - when cast to a map only of the tuples in the list will remain
            case t: JsArray => t.as[List[String]].map((key, _))
            case t: JsString => List((key, t.as[String]))
            case _ => throw new IllegalArgumentException(s"Unsupported type: ${jsonObject.getClass}")
        }
    }

    /**
      * Extracts a String value from a JSON object.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return String value of the JSON field
      */
    def extractString(json: JsValue, path: List[String]): Option[String] = {
        getValue(json, path).map(_.as[String])
    }

    /**
      * Extracts a Double value from a JSON object.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return Double value of the JSON field as String
      */
    def extractDouble(json: JsValue, path: List[String]): Option[String] = {
        getValue(json, path).map(_.as[Double].toString)
    }

    /**
      * Extracts a Boolean value from a JSON object.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return Boolean value of the JSON field
      */
    def extractBoolean(json: JsValue, path: List[String]): Option[Boolean] = {
        getValue(json, path).map(_.as[Boolean])
    }

    /**
      * Extracts a timestamp as Date from a JSON object.
      * @param json JSON object containing the data
      * @param path JSON path of the object fields to traverse
      * @return Date object of the timestamp in the JSON field
      */
    def extractDate(json: JsValue, path: List[String], datePattern: String): Option[Date] = {
        getValue(json, path).map { dateString =>
            val dateFormat = new SimpleDateFormat(datePattern)
            dateFormat.parse(dateString.as[String])
        }
    }
}
