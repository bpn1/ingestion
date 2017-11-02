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

package de.hpi.ingestion.deduplication.similarity

import java.net.URL


/**
  * Computes the similarity of two Hostnames from two given urls
  */
object UrlCompare extends SimilarityMeasure[String] {


    /**
      * Extracts the hosts name out of a given url string. A possible "www." subdomain will get stripped
      * @param s a url string
      * @return the urls hostname as a string
      */
    def cleanUrlString(s: String) : String = {
        new URL(s).getHost.replaceFirst("www.","")
    }

    /**
      * Calculates the similariity score for two hostnames extracted from given urls based on JaroWinkler
      * @param s some url
      * @param t another url
      * @param u no specific use in here
      * @return a normalized similarity score between 1.0 and 0.0 or
      * 0.0 as default value if one of the input strings is empty
      */
    override def compare(s: String, t: String, u: Int = 1) : Double = {
        JaroWinkler.compare(
            cleanUrlString(s),
            cleanUrlString(t)
        )
    }
}
