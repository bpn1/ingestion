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

package de.hpi.ingestion.deduplication

import de.hpi.ingestion.deduplication.models.config.SimilarityMeasureConfig
import de.hpi.ingestion.deduplication.similarity.SimilarityMeasure

/**
  * This class is a factory of comparing methods for different kinds of values.
  */
object CompareStrategy extends Serializable {
    /**
      * This method compares two string-lists without case sensitivity.
      * @param leftValue One-element string-list to be compared with rightValue
      * @param rightValue One-element string-list to be compared with leftValue
      * @return The similarity score of the two input strings as double
      */
    def caseInsensitiveCompare(
        leftValue: List[String],
        rightValue: List[String],
        scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]
    ): Double = {
        defaultCompare(leftValue.map(_.toLowerCase), rightValue.map(_.toLowerCase), scoreConfig)
    }

    /**
      * This method compares two string-lists with one element each.
      * @param leftValue One-element string-list to be compared with rightValue
      * @param rightValue One-element string-list to be compared with leftValue
      * @return The similarity score of the two input strings as double
      */
    def singleStringCompare(
        leftValue: List[String],
        rightValue: List[String],
        scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]
    ): Double = {
        scoreConfig.compare(leftValue.head, rightValue.head)
    }

    /**
      * This method compares two string-lists by default.
      * @param leftValues List of strings to be compared with rightValues
      * @param rightValues List of strings to be compared with leftValues
      * @return The similarity score of the two input lists of strings as double
      */
    def defaultCompare(
        leftValues: List[String],
        rightValues: List[String],
        scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]
    ): Double = {
        val scoreSum = leftValues
            .map { leftValue =>
                rightValues
                    .map(rightValue => scoreConfig.compare(leftValue, rightValue))
                    .max
            }.sum
        scoreSum / leftValues.size
    }

    /**
      * Factory to find the fitting function to compare the given attribute.
      * @param attribute String defining the method to apply.
      * @return Function to use for comparison.
      */
    def apply(
        attribute: String
    ): (List[String], List[String], SimilarityMeasureConfig[String, SimilarityMeasure[String]]) => Double = {
        attribute match {
            case "name"
                 | "geo_country"
                 | "geo_city"
                 | "aliases" => caseInsensitiveCompare
            case "category"
                 | "geo_postal"
                 | "gen_employees" => singleStringCompare
            case _ => defaultCompare
        }
    }
}
