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

import org.scalatest.{FlatSpec, Matchers}

class JaroUnitTest extends FlatSpec with Matchers {

	"compare" should "return the Jaro score for given strings" in {
		// results confirmed by https://asecuritysite.com/forensics/simstring
		val testData = List(
			("dwayne", "duane", 0.8222222222222223), // result confirmed in round terms
			("jones", "johnson", 0.7904761904761904), // result confirmed in round terms
			("fvie", "ten", 0.0))

		testData.foreach(tuple =>
			Jaro.compare(tuple._1, tuple._2) shouldEqual tuple._3)
	}
}
