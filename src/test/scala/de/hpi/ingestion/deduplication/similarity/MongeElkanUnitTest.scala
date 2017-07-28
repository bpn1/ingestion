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

import com.rockymadden.stringmetric.similarity.JaroWinklerMetric
import org.scalatest.{FlatSpec, Matchers}

class MongeElkanUnitTest extends FlatSpec with Matchers {
	"score" should "return the MongeElkan score for given strings" in {
		val data = List(
			(Array("Peter", "Christen"), Array("Christian", "Pedro"), 0.8586111111111111),
			(Array("paul", "johnson"), Array("johson", "paule"), 0.9438888888888889)
		)
		data.foreach { case (s, t, score) =>
			MongeElkan.score(s, t) shouldEqual score
		}
	}

	it should "return the JaroWinkler score if tokens contain only one element" in {
		val data = (Array("Audi"), Array("Audio"))
		val expected = JaroWinklerMetric.compare(data._1(0), data._2(0)).getOrElse(0.0)
		MongeElkan.score(data._1, data._2) shouldEqual expected
	}

	"compare" should "return the MongeElkan score for given strings" in {
		val data = List(
			("Peter Christen", "Christian Pedro", 0.8586111111111111),
			("paul johnson", "johson paule", 0.9438888888888889)
		)
		data.foreach { case (s, t, score) =>
			MongeElkan.compare(s, t) shouldEqual score
		}
	}

	it should "be symmetric" in {
		val token = ("Audi AG", "Audi")
		val score = MongeElkan.compare(token._1, token._2)
		val symScore = MongeElkan.compare(token._2, token._1)
		score shouldEqual symScore
		score shouldEqual 1.0
	}
}
