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

import java.lang.Math.sqrt
import java.net.URL

import com.rockymadden.stringmetric.similarity._

package object similarity {
    trait SimilarityMeasure[-T] extends Serializable {
        /**
          * Calculates a similarity score for two objects.
          * @param x object to be compared to y
          * @param y object to be compared to x
          * @param u is for giving config params to similarity measures that take one
          * @return a normalized similarity score between 0.0 and 1.0
          */
        def compare(x: T, y: T, u: Int = 1) : Double
    }

    object SimilarityMeasure {
        val dataTypes: Map[String, SimilarityMeasure[_]] = Map(
            "ExactMatch" -> ExactMatch,
            "MongeElkan" -> MongeElkan,
            "Jaccard" -> Jaccard,
            "DiceSorensen" -> DiceSorensen,
            "Jaro" -> Jaro,
            "JaroWinkler" -> JaroWinkler,
            "N-Gram" -> NGram,
            "Overlap" -> Overlap,
            "EuclideanDistance" -> EuclideanDistance,
            "RelativeNumbersSimilarity" -> RelativeNumbersSimilarity,
            "UrlCompare" -> UrlCompare
        )

        /**
          * Returns a Similarity Measure given its name. If there is no Similarity Measure with the given name then
          * the default Similarity Measure Exact Match String is returned.
          * @param similarityMeasure name of the Similarity Measure
          * @tparam T type of the Similarity Measure
          * @return the requested Similarity Measure if it exists or else Exact Match String as default
          */
        def get[T](similarityMeasure: String): SimilarityMeasure[T] = {
            dataTypes.getOrElse(similarityMeasure, ExactMatch).asInstanceOf[SimilarityMeasure[T]]
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /////////////////////////// GENERAL SIMILARITIES //////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    /**
      * Checks if the given objects are the same. Returns 1.0 if they are and 0.0 if they are not.
      */
    object ExactMatch extends SimilarityMeasure[Any] {
        override def compare(x: Any, y: Any, u: Int = 1) = if(x == y) 1.0 else 0.0
    }

    ///////////////////////////////////////////////////////////////////////////
    /////////////////////////// NUMBER SIMILARITIES ///////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    /**
      * A similarity measure that computes how close two given numbers are to each other in terms of percentage.
      */
    object RelativeNumbersSimilarity extends SimilarityMeasure[String] {
        override def compare(x: String, y: String, u: Int = 1) : Double = {
            val max = Math.max(x.toDouble, y.toDouble)
            val min = Math.min(x.toDouble, y.toDouble)
            min / max
        }
    }

    /**
      * Compares two geographical coordinates based on their euclidean distance on the globe.
      */
    object EuclideanDistance extends SimilarityMeasure[String] {
        val degreeLength = 110.25
        /**
          * Computes a the distance in km between two geographical coordinates.
          * http://jonisalonen.com/2014/computing-distance-between-coordinates-can-be-simple-and-fast/
          * @param point1 latitude and longitude of the first point
          * @param point2 latitude and longitude of the second point
          * @return distance in km
          */
        def computeDistance(point1: (Double, Double), point2: (Double, Double)) : Double = {
            val (lat1, long1) = point1
            val (lat2, long2) = point2
            val deltaLat = lat2 - lat1
            val deltaLong = (long2 - long1) * Math.cos(lat1)
            degreeLength * sqrt(deltaLat * deltaLat + deltaLong * deltaLong)
        }

        /**
          * Bins the distance into different scores according to thresholds.
          * @param distance the distance
          * @param scale scale of the thresholds
          * @return score of the distance, which is in {0.0, 0.5, 0.75, 1.0}
          */
        def turnDistanceIntoScore(distance: Double, scale: Int = 1): Double = {
            distance match {
                case x if x <= (5.0 * scale) => 1.0
                case x if x <= (20.0 * scale) => 0.75
                case x if x <= (100.0 * scale) => 0.5
                case _ => 0.0
            }
        }

        /**
          * Compares two geographical coordinates based on the euclidian distance between them.
          * @param x geographical coordinates separated by a semicolon
          * @param y geographical coordinates separated by a semicolon
          * @param u scale of the threshold distances
          * @return a normalized similarity score between 0.0 and 1.0
          */
        override def compare(x: String, y: String, u: Int = 1) : Double = {
            val Array(lat1, long1) = x.split(";").map(_.toDouble)
            val Array(lat2, long2) = y.split(";").map(_.toDouble)
            val distance = computeDistance((lat1, long1), (lat2, long2))
            turnDistanceIntoScore(distance, u)
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    /////////////////////////// STRING SIMILARITIES ///////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    /**
      * Calculates the similariity score for two hostnames extracted from given urls based on JaroWinkler
      */
    object UrlCompare extends SimilarityMeasure[String] {
        /**
          * Extracts the hosts name out of a given url string. A possible "www." subdomain will get stripped
          * @param url a url string
          * @return the urls hostname as a string
          */
        def cleanUrlString(url: String) : String = {
            new URL(url).getHost.replaceFirst("www.","")
        }

        override def compare(x: String, y: String, u: Int = 1) : Double = {
            JaroWinkler.compare(cleanUrlString(x), cleanUrlString(y), u)
        }
    }

    /**
      * An implementation of the Monge-Elkan algorithm.
      */
    object MongeElkan extends SimilarityMeasure[String] {
        def score(sToken: Array[String], tToken: Array[String]): Double = {
            sToken.map { token =>
                tToken.map(JaroWinklerMetric.compare(token, _).getOrElse(0.0)).max
            }.sum / sToken.length.toDouble
        }

        override def compare(x: String, y: String, u: Int = 1) : Double = {
            val token = (x.split(" "), y.split(" "))
            val tupledScore = (score _).tupled
            List(tupledScore(token), tupledScore(token.swap)).max
        }
    }

    /**
      * Trait for the similarity measures wrapping the rocky madden string metrics.
      */
    trait RockyMaddenStringMetric extends SimilarityMeasure[String] {
        def stringMetric(u: Int): (String, String) => Option[Double]

        override def compare(x: String, y: String, u: Int): Double = {
            stringMetric(u)(x, y).getOrElse(0.0)
        }
    }

    /**
      * Trait for the similarity measures wrapping the rocky madden string metrics that produce integer results.
      */
    trait RockyMaddenStringMetricInt extends RockyMaddenStringMetric {
        def stringIntMetric(u: Int): (String, String) => Option[Int]

        override def stringMetric(u: Int): (String, String) => Option[Double] = {
            val optionMap: Option[Int] => Option[Double] = _.map(_.toDouble)
            val stringMetricDouble: (String, String) => Option[Double] = {
                (x, y) => stringIntMetric(u).tupled.andThen(optionMap)((x, y))
            }
            stringMetricDouble
        }
    }

    object DiceSorensen extends RockyMaddenStringMetric {
        override def stringMetric(u: Int): (String, String) => Option[Double] = DiceSorensenMetric(u).compare
    }

    object Hamming extends RockyMaddenStringMetricInt {
        override def stringIntMetric(u: Int): (String, String) => Option[Int] = HammingMetric.compare
    }

    object Jaccard extends RockyMaddenStringMetric {
        override def stringMetric(u: Int): (String, String) => Option[Double] = JaccardMetric(u).compare
    }

    object Jaro extends RockyMaddenStringMetric {
        override def stringMetric(u: Int): (String, String) => Option[Double] = JaroMetric.compare
    }

    object JaroWinkler extends RockyMaddenStringMetric {
        override def stringMetric(u: Int): (String, String) => Option[Double] = JaroWinklerMetric.compare
    }

    object Levenshtein extends RockyMaddenStringMetricInt {
        override def stringIntMetric(u: Int): (String, String) => Option[Int] = LevenshteinMetric.compare
    }

    object NGram extends RockyMaddenStringMetric {
        override def stringMetric(u: Int): (String, String) => Option[Double] = NGramMetric(u).compare
    }

    object Overlap extends RockyMaddenStringMetric {
        override def stringMetric(u: Int): (String, String) => Option[Double] = OverlapMetric(u).compare
    }

    object RatcliffObershelp extends RockyMaddenStringMetric {
        override def stringMetric(u: Int): (String, String) => Option[Double] = RatcliffObershelpMetric.compare
    }
}
