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

package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._


class DBpediaRelationParserTest extends FlatSpec with SharedSparkContext with Matchers with RDDComparisons {

	"Translation triple" should "be parsed into map from english to german wikipedia titles" in {
		val ttl = sc.parallelize(TestData.interlanguageLinksEn())
		val prefixesList = TestData.prefixesList
		DBpediaRelationParser.getGermanLabels(ttl, prefixesList) shouldEqual TestData.germanLabels()
	}

	"The parsed relations" should "be exactly these" in {
		val labels = sc.parallelize(TestData.interlanguageLinksEn())
		val dbpedia = sc.parallelize(TestData.dbpediaRawRelations())
		val input = List(dbpedia).toAnyRDD() ++ List(labels).toAnyRDD()
		val relations = DBpediaRelationParser.run(input, sc)
			.fromAnyRDD[Relation]()
			.head
			.collect
			.toList
		relations shouldEqual TestData.dbpediaParsedRelations()
	}
}
