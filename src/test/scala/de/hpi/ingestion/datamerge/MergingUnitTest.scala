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
import org.scalatest.{FlatSpec, Matchers}

class MergingUnitTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons {
    "Master Nodes" should "contain relevant attributes" in {
        val job = new Merging
        job.stagedSubjects = sc.parallelize(TestData.staging)
        job.subjects = sc.parallelize(TestData.subjects)
        job.duplicates = sc.parallelize(TestData.duplicates)
        job.run(sc)
        val output = job.mergedSubjects.collect.toList
        val (existingMaster, newMaster) = output
            .filter(_.isMaster)
            .partition(master => TestData.idList.contains(master.id))
        existingMaster
            .foreach { master =>
                val properties = master
                    .properties
                    .mapValues(_.sorted)
                val expectedProperties = TestData
                    .mergedSubjects
                    .find(_ == master)
                    .get
                    .properties
                    .mapValues(_.sorted)
                properties shouldEqual expectedProperties
            }
        val newMasterAttributes = newMaster
            .map(_.properties.mapValues(_.sorted))
            .toSet
        val expectedAttributes = Set(TestData.mergedSubjects(8).properties, TestData.mergedSubjects(10).properties)
        newMasterAttributes shouldEqual expectedAttributes
    }

    it should "contain relevant relations" in {
        val job = new Merging
        job.stagedSubjects = sc.parallelize(TestData.staging)
        job.subjects = sc.parallelize(TestData.subjects)
        job.duplicates = sc.parallelize(TestData.duplicates)
        job.run(sc)
        val output = job.mergedSubjects.collect.toList
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
            TestData.mergedSubjects(10).masterRelations)

        newMasterAttributes shouldEqual expectedAttributes
    }

    it should "contain a master relations for each slave" in {
        val job = new Merging
        job.stagedSubjects = sc.parallelize(TestData.staging)
        job.subjects = sc.parallelize(TestData.subjects)
        job.duplicates = sc.parallelize(TestData.duplicates)
        job.run(sc)
        val relationsList = job
            .mergedSubjects
            .groupBy(_.master)
            .map { case (id, subjects) => subjects.partition(_.id == id) }
            .map(x => (x._1.head, x._2.map(_.id)))
            .collect

        relationsList.foreach { case (master, slaves) =>
            master.slaves.toSet shouldEqual slaves.toSet
        }
    }

    "Slaves" should "update their master" in {
        val job = new Merging
        job.stagedSubjects = sc.parallelize(TestData.staging)
        job.subjects = sc.parallelize(TestData.subjects)
        job.duplicates = sc.parallelize(TestData.duplicates)
        job.run(sc)
        val relationsList = job
            .mergedSubjects
            .groupBy(_.master)
            .map { case (id, subjects) => subjects.partition(_.id == id) }
            .map(x => (x._1.head.id, x._2.map(_.master)))
            .collect

        relationsList.foreach { case (master, slaves) =>
            slaves.forall(_ == master) shouldBe true
        }
    }

    it should "contain a relation to their duplicate" in {
        val job = new Merging
        job.stagedSubjects = sc.parallelize(TestData.staging)
        job.subjects = sc.parallelize(TestData.subjects)
        job.duplicates = sc.parallelize(TestData.duplicates)
        job.run(sc)
        val duplicateRelations = job
            .mergedSubjects
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

    "Duplicate matches from the same datasource" should "be aggregated" in {
        val job = new Merging
        job.subjects = sc.parallelize(TestData.subjects.take(2))
        job.stagedSubjects = sc.parallelize(TestData.staging.take(2))
        job.duplicates = sc.parallelize(TestData.duplicatesWithMultipleMatches)
        job.run(sc)
        val mergedSubjects = job.mergedSubjects.collect.toList.sortBy(_.id)
        val expectedSubjects = TestData.mergedSubjectsWithMultipleMatches.sortBy(_.id)
        mergedSubjects.zip(expectedSubjects).foreach { case (mergedSubject, expectedSubject) =>
            mergedSubject.id shouldEqual expectedSubject.id
            mergedSubject.master shouldEqual expectedSubject.master
            mergedSubject.datasource shouldEqual expectedSubject.datasource
            mergedSubject.name shouldEqual expectedSubject.name
            mergedSubject.aliases shouldEqual expectedSubject.aliases
            mergedSubject.category shouldEqual expectedSubject.category
            mergedSubject.properties shouldEqual expectedSubject.properties
            mergedSubject.relations shouldEqual expectedSubject.relations
        }
    }

    they should "result in the best scored match" in {
        val job = new Merging
        job.subjects = sc.parallelize(TestData.subjects)
        job.stagedSubjects = sc.parallelize(TestData.staging.take(3))
        job.duplicates = sc.parallelize(TestData.duplicatesWithConflictingMatches)
        job.run(sc)
        val mergedSubjects = job.mergedSubjects.collect.toList.sortBy(_.id)
        val expectedSubjects = TestData.mergedSubjectsWithConflictingMatches.sortBy(_.id)
        mergedSubjects.zip(expectedSubjects).foreach { case (mergedSubject, expectedSubject) =>
            mergedSubject.id shouldEqual expectedSubject.id
            mergedSubject.master shouldEqual expectedSubject.master
            mergedSubject.datasource shouldEqual expectedSubject.datasource
            mergedSubject.name shouldEqual expectedSubject.name
            mergedSubject.aliases shouldEqual expectedSubject.aliases
            mergedSubject.category shouldEqual expectedSubject.category
            mergedSubject.properties shouldEqual expectedSubject.properties
            mergedSubject.relations shouldEqual expectedSubject.relations
        }
    }

    "Duplicates matching to different Subjects" should "be reduced to the match with the highest score" in {
        val duplicates = sc.parallelize(TestData.duplicatesWithConflictingMatches)
        val reducedDuplicates = Merging.deduplicateMatches(duplicates)
            .collect
            .toList
            .map(duplicate => duplicate.copy(candidates = duplicate.candidates.sortBy(_.id)))
            .toSet
        val expectedFixedDuplicates = TestData.duplicatesWithFixedConflicts
            .map(duplicate => duplicate.copy(candidates = duplicate.candidates.sortBy(_.id)))
            .toSet
        reducedDuplicates shouldEqual expectedFixedDuplicates
    }
}
