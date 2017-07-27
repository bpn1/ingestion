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

package de.hpi.ingestion.datamerge

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class MergingUnitTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {
	"Master Nodes" should "contain relevant attributes" in {
		val staging = sc.parallelize(TestData.staging)
		val subjects = sc.parallelize(TestData.subjects)
		val duplicates = sc.parallelize(TestData.duplicates)
		val input = List(subjects, staging).toAnyRDD() ::: List(duplicates).toAnyRDD()
		val output = Merging.run(input, sc).head.asInstanceOf[RDD[Subject]].collect.toList

		val (existingMaster, newMaster) = output
			.filter(_.isMaster)
			.partition(master => TestData.idList.contains(master.id))

		existingMaster
			.map { master =>
				(
					master
						.properties
				    	.mapValues(_.sorted),
					TestData
						.mergedSubjects
						.find(_ == master)
						.get
						.properties
						.mapValues(_.sorted)
				)
			}
	    	.foreach { case (properties, expected) =>
				properties shouldEqual expected
			}

		val newMasterAttributes = newMaster
			.map(_.properties.mapValues(_.sorted))
			.toSet

		val expectedAttributes = Set(TestData.mergedSubjects(8).properties, TestData.mergedSubjects(10).properties)

		newMasterAttributes shouldEqual expectedAttributes
	}

	it should "contain relevant relations" in {
		val staging = sc.parallelize(TestData.staging)
		val subjects = sc.parallelize(TestData.subjects)
		val duplicates = sc.parallelize(TestData.duplicates)
		val input = List(subjects, staging).toAnyRDD() ::: List(duplicates).toAnyRDD()
		val output = Merging.run(input, sc).head.asInstanceOf[RDD[Subject]].collect.toList

		val (existingMaster, newMaster) = output
			.filter(_.isMaster)
			.partition(master => TestData.idList.contains(master.id))

		existingMaster
			.map(master => (master.masterRelations, TestData.mergedSubjects.find(_ == master).get.masterRelations))
			.foreach { case (relations, expected) =>
				relations shouldEqual expected
			}

		val newMasterAttributes = newMaster
			.map(_.masterRelations)
	    	.toSet

		val expectedAttributes = Set(
			TestData.mergedSubjects(8).masterRelations,
			TestData.mergedSubjects(10).masterRelations
		)

		newMasterAttributes shouldEqual expectedAttributes
	}

	it should "contain a master relations for each slave" in {
		val staging = sc.parallelize(TestData.staging)
		val subjects = sc.parallelize(TestData.subjects)
		val duplicates = sc.parallelize(TestData.duplicates)
		val input = List(subjects, staging).toAnyRDD() ::: List(duplicates).toAnyRDD()
		val output = Merging.run(input, sc).head.asInstanceOf[RDD[Subject]]

		val relationsList = output
			.groupBy(_.master)
			.map { case (id, subjects) => subjects.partition(_.id == id) }
			.map(x => (x._1.head, x._2.map(_.id)))
			.collect

		relationsList.foreach { case (master, slaves) =>
			master.slaves.toSet shouldEqual slaves.toSet
		}
	}

	"Slaves" should "update their master" in {
		val staging = sc.parallelize(TestData.staging)
		val subjects = sc.parallelize(TestData.subjects)
		val duplicates = sc.parallelize(TestData.duplicates)
		val input = List(subjects, staging).toAnyRDD() ::: List(duplicates).toAnyRDD()
		val output = Merging.run(input, sc).head.asInstanceOf[RDD[Subject]]

		val relationsList = output
			.groupBy(_.master)
			.map { case (id, subjects) => subjects.partition(_.id == id) }
			.map(x => (x._1.head.id, x._2.map(_.master)))
			.collect

		relationsList.foreach { case (master, slaves) =>
			slaves.forall(_ == master) shouldBe true
		}
	}

	it should "contain a relation to their duplicate" in {
		val staging = sc.parallelize(TestData.staging)
		val subjects = sc.parallelize(TestData.subjects)
		val duplicates = sc.parallelize(TestData.duplicates)
		val input = List(subjects, staging).toAnyRDD() ::: List(duplicates).toAnyRDD()
		val output = Merging.run(input, sc).head.asInstanceOf[RDD[Subject]]

		val duplicateRelations = output
			.flatMap(_.relations.filter(_._2.contains(SubjectManager.duplicateKey)))
			.mapValues(_.filter(_._1 == SubjectManager.duplicateKey))
			.collect
			.toList
			.groupBy(_._1)
	    	.mapValues(_.toSet)

		val expectedRelations = TestData
			.mergedSubjects
			.flatMap(_.relations.filter(_._2.contains(SubjectManager.duplicateKey)))
			.map(x => x._1 -> x._2.filter(_._1 == SubjectManager.duplicateKey))
			.groupBy(_._1)
			.mapValues(_.toSet)

		duplicateRelations shouldEqual expectedRelations
	}
}
