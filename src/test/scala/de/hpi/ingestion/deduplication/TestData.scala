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

import java.util.UUID
import de.hpi.ingestion.datalake.models.{Subject, Version}
import de.hpi.ingestion.deduplication.models.{BlockEvaluation, FeatureEntry, PrecisionRecallDataTuple}
import de.hpi.ingestion.deduplication.blockingschemes.ListBlockingScheme
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.deduplication.models.config.{AttributeConfig, SimilarityMeasureConfig}
import de.hpi.ingestion.deduplication.similarity._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

// scalastyle:off line.size.limit
// scalastyle:off number.of.methods
// scalastyle:off file.size.limit
object TestData {
    val datasource = "testSource"
    val idList = List.fill(8)(UUID.randomUUID())
    def testVersion(sc: SparkContext): Version = Version("SomeTestApp", Nil, sc, false, None)

    def bucketsList: Map[Int, List[Double]] = Map(
        10 -> List(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0),
        5 -> List(0.0, 0.2, 0.4, 0.6, 0.8, 1.0)
    )

    def testScores: List[Map[Double, Double]] = List(
        Map(0.5 -> 0.5, 0.79 -> 0.7),
        Map(0.5 -> 0.4, 0.79 -> 0.6)
    )

    def testData(sc: SparkContext): RDD[((UUID, UUID), Double)] = {
        sc.parallelize(Seq(
            ((idList.head, idList(1)), 1.0),
            ((idList(2), idList(3)), 1.0),
            ((idList(4), idList(5)), 1.0),
            ((idList(6), idList(7)), 1.0)
        ))
    }

    def trainingData(sc: SparkContext): RDD[((UUID, UUID), Double)] = {
        sc.parallelize(Seq(
            ((idList.head, idList(1)), 0.7),
            ((idList(2), idList(3)), 0.8),
            ((idList(4), idList(7)), 0.5),
            ((idList(6), idList(5)), 0.6)
        ))
    }

    def testData2(sc: SparkContext): RDD[(UUID, UUID)] = {
        sc.parallelize(Seq(
            (idList.head, idList(1)),
            (idList(2), idList(3)),
            (idList(4), idList(5)),
            (idList(6), idList(7))
        ))
    }

    def trainingCandidates(sc: SparkContext): RDD[Duplicates] = {
        sc.parallelize(Seq(
            Duplicates(idList.head, Option("test0"), "Source", List(Candidate(idList(1), Option("test1"), 0.7))),
            Duplicates(idList(2), Option("test2"), "Source", List(Candidate(idList(3), Option("test3"), 0.8))),
            Duplicates(idList(4), Option("test4"), "Source", List(Candidate(idList(7), Option("test7"), 0.5))),
            Duplicates(idList(6), Option("test6"), "Source", List(Candidate(idList(5), Option("test5"), 0.6)))
        ))
    }

    def truePositives(sc: SparkContext): RDD[(Double, Double)] = {
        sc.parallelize(List((0.7, 1.0), (0.8, 1.0)))
    }

    def falsePositives(sc: SparkContext): RDD[(Double, Double)] = {
        sc.parallelize(List((0.5, 0.0), (0.6, 0.0)))
    }

    def falseNegatives(sc: SparkContext): RDD[(Double, Double)] = {
        sc.parallelize(List.fill(2)((0: Double, 1: Double)))
    }

    def labeledPoints(sc: SparkContext): RDD[(Double, Double)] = {
        truePositives(sc).union(falsePositives(sc)).union(falseNegatives(sc))
    }

    def precisionRecallResults: List[PrecisionRecallDataTuple] = List(
        PrecisionRecallDataTuple(0.0,0.6666666666666666,1.0,0.8),
        PrecisionRecallDataTuple(0.1,0.5,0.5,0.5),
        PrecisionRecallDataTuple(0.2,0.5,0.5,0.5),
        PrecisionRecallDataTuple(0.3,0.5,0.5,0.5),
        PrecisionRecallDataTuple(0.4,0.5,0.5,0.5),
        PrecisionRecallDataTuple(0.5,0.5,0.5,0.5),
        PrecisionRecallDataTuple(0.6,0.6666666666666666,0.5,0.5714285714285715),
        PrecisionRecallDataTuple(0.7,1.0,0.5,0.6666666666666666),
        PrecisionRecallDataTuple(0.8,1.0,0.25,0.4),
        PrecisionRecallDataTuple(0.9,0.0,0.0,0.0),
        PrecisionRecallDataTuple(1.0,0.0,0.0,0.0)
    )

    def similarityMeasureStats: SimilarityMeasureStats = SimilarityMeasureStats(
        data = List(
            PrecisionRecallDataTuple(0.0,0.6666666666666666,1.0,0.8),
            PrecisionRecallDataTuple(0.2,0.5,0.5,0.5),
            PrecisionRecallDataTuple(0.4,0.5,0.5,0.5),
            PrecisionRecallDataTuple(0.6,0.6666666666666666,0.5,0.5714285714285715),
            PrecisionRecallDataTuple(0.8,1.0,0.25,0.4),
            PrecisionRecallDataTuple(1.0,0.0,0.0,0.0)
        ),
        comment = Option("Naive Deduplication")
    )

    val dbpediaList = List(
        Subject(
            master = null,
            datasource = datasource,
            name = Option("dbpedia_1"),
            properties = Map("id_dbpedia" -> List("dbpedia_1"), "id_wikidata" -> List("Q1"))
        ),
        Subject(
            master = null,
            datasource = datasource,
            name = Option("dbpedia_2"),
            properties = Map("id_dbpedia" -> List("dbpedia_2"), "id_wikidata" -> List("Q2"))
        ),
        Subject(
            master = null,
            datasource = datasource,
            name = Option("dbpedia_3"),
            properties = Map("id_wikidata" -> List("Q3"))
        ),
        Subject(
            master = null,
            datasource = datasource,
            name = Option("dbpedia_4")
        )
    )

    val wikidataList = List(
        Subject(
            master = null,
            datasource = datasource,
            name = Option("wikidata_1"),
            properties = Map("id_dbpedia" -> List("dbpedia_1"), "id_wikidata" -> List(""))
        ),
        Subject(
            master = null,
            datasource = datasource,
            name = Option("wikidata_2"),
            properties = Map("id_dbpedia" -> List("dbpedia_2"), "id_wikidata" -> List("Q2"))
        ),
        Subject(
            master = null,
            datasource = datasource,
            name = Option("wikidata_3"),
            properties = Map("id_dbpedia" -> List("dbpedia_3"), "id_wikidata" -> List("Q3"))
        ),
        Subject(
            master = null,
            datasource = datasource,
            name = Option("wikidata_4")
        )
    )

    def propertyKeyDBpedia(sc: SparkContext): RDD[(String, Subject)] = {
        sc.parallelize(Seq(
            ("dbpedia_1", dbpediaList.head),
            ("dbpedia_2", dbpediaList(1))
        ))
    }

    def joinedWikidata(sc: SparkContext): RDD[(UUID, UUID)] = {
        sc.parallelize(Seq(
            (dbpediaList(1).id, wikidataList(1).id),
            (dbpediaList(2).id, wikidataList(2).id)
        ))
    }

    def joinedDBpediaWikidata(sc: SparkContext): RDD[(UUID, UUID)] = {
        sc.parallelize(Seq(
            (dbpediaList.head.id, wikidataList.head.id),
            (dbpediaList(1).id, wikidataList(1).id),
            (dbpediaList(2).id, wikidataList(2).id)
        ))
    }

    def subjectBlocks(sc: SparkContext): RDD[Block] = {
        sc.parallelize(Seq(
            Block(key = "Audi ", subjects = List(subjects(1)), staging = List(stagings(1))),
            Block(key = "Volks", subjects = subjects.take(1), staging = stagings.take(1)),
            Block(key = "undefined", subjects = subjects.takeRight(2), staging = Nil)
        ))
    }

    def filteredDuplicates(sc: SparkContext): RDD[(Subject, Subject, Double)] = {
        sc.parallelize(Seq(
            (subjects.head, stagings.head, 0.9769230769230769),
            (subjects(1), stagings(1), 1.0)
        ))
    }

    def testDuplicates(sc: SparkContext): RDD[(Subject, Subject, Double)] = {
        sc.parallelize(Seq(
            (subjects.head, stagings.head, 0.9769230769230769),
            (subjects(1), stagings(1), 1.0),
            (subjects(1), stagings.head, 0.5)
        ))
    }

    def smallerDuplicateStats: (Traversable[(UUID, UUID)], (String, BlockStats)) = {
        (
            List(
                (UUID.fromString("974c4495-52fd-445c-b09b-8b769bfb4212"), UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130")),
                (UUID.fromString("f2d98c16-2ac2-4cb7-bd65-4fcb17178060"), UUID.fromString("413c5711-67d0-1151-1077-b000000000b5"))
            ), ("tag", BlockStats("key", 4, 2, 0.25))
        )
    }

    def createdDuplicateCandidates(subjects: List[Subject], stagings: List[Subject]): List[Duplicates] = {
        val stagingTable = "subject_wikidata"
        List(
            Duplicates(
                subjects.head.id,
                subjects.head.name,
                stagingTable,
                List(Candidate(stagings.head.id, stagings.head.name, 0.9769230769230769))
            ),
            Duplicates(
                subjects(1).id,
                subjects(1).name,
                stagingTable,
                List(Candidate(stagings(1).id, stagings(1).name, 1.0), Candidate(stagings.head.id, stagings.head.name, 0.5))
            )
        )
    }

    def duplicateStats(): List[(List[(UUID, UUID)], (String, BlockStats))] = {
        List(
            (
                List(
                    (UUID.fromString("974c4495-52fd-445c-b09b-8b769bfb4212"), UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130")),
                    (UUID.fromString("0ef813fa-55d9-4712-a41d-46a810fca8c9"), UUID.fromString("413c5711-67d0-1151-1077-b000000000b5"))
                ),
                ("key1", BlockStats("tag1",4, 2, 0.25))
            ),
            (
                List((UUID.fromString("0ef813fa-55d9-4712-a41d-46a810fca8c9"), UUID.fromString("413c5711-67d0-1151-1077-b000000000b5"))),
                ("key2", BlockStats("tag2",1, 1, 1.0))
            )
        )
    }

    def expectedPairwiseCompleteness: Map[String, Double] = {
        Map("key1" -> 1.0, "key2" -> 0.5, "sum" -> 1.0)
    }

    def expectedBlockCount: Map[String, Int] = {
        Map("testTag1" -> 2, "testTag2" -> 1, "sum" -> 3)
    }

    def expectedCompareCount: Map[String, BigInt] = {
        Map("testTag1" -> 5, "testTag2" -> 15,"sum" -> 20)
    }

    def blocks(): List[(String, Block)] = {
        List(
            ("testTag1", Block(
                key = "anyKey",
                subjects = List(idList.head).map(Subject.master),
                staging = List(idList.last).map(Subject.master)
            )
            ),
            ("testTag2", Block(
                key = "Anakin",
                subjects = List(idList.head, idList(1), idList(2), idList(3), idList(4)).map(Subject.master),
                staging = List(idList.last, idList(4), idList(6)).map(Subject.master))
            ),
            ("testTag1", Block(
                key = "anotherKey",
                subjects = List(idList(4), idList(5)).map(Subject.master),
                staging = List(idList(6), idList.last).map(Subject.master))
            )
        )
    }

    def trueDuplicates(subjects: List[Subject], stagings: List[Subject]): List[Duplicates] = {
        val stagingTable = "subject_wikidata"
        List(
            Duplicates(
                subjects.head.id,
                subjects.head.name,
                stagingTable,
                List(Candidate(stagings.head.id, stagings.head.name, 0.9769230769230769))
            ),
            Duplicates(
                subjects(1).id,
                subjects(1).name,
                stagingTable,
                List(Candidate(stagings(1).id, stagings(1).name, 1.0))
            )
        )
    }

    def testConfig(attribute: String = "name"): List[AttributeConfig] = List(
        AttributeConfig(attribute, 1.0, List(SimilarityMeasureConfig(MongeElkan, 0.5), SimilarityMeasureConfig(JaroWinkler, 0.5)))
    )

    def simpleTestConfig: List[AttributeConfig] = List(
        AttributeConfig("name", 1.0, List(SimilarityMeasureConfig(MongeElkan, 1.0)))
    )

    def complexTestConfig: List[AttributeConfig] = List(
        AttributeConfig("name", 0.7, List(SimilarityMeasureConfig(MongeElkan, 0.5), SimilarityMeasureConfig(JaroWinkler, 0.5))),
        AttributeConfig("geo_city", 0.1, List(SimilarityMeasureConfig(Jaccard, 1))),
        AttributeConfig("geo_coords", 0.1, List(SimilarityMeasureConfig(EuclidianDistance, 1))),
        AttributeConfig("gen_income", 0.1, List(SimilarityMeasureConfig(RelativeNumbersSimilarity, 1)))
    )

    def testConfigScore: Double = {
        (MongeElkan.compare("Volkswagen", "Volkswagen AG") + JaroWinkler.compare("Volkswagen", "Volkswagen AG")) * 0.5
    }

    def simpleTestConfigScore: Double = {
        MongeElkan.compare("Volkswagen", "Volkswagen AG")
    }

    def complexTestConfigScore: Double = 0.7439095102628327

    def testCompareScore(attribute: String, subject1: Subject, subject2: Subject, simMeasure: SimilarityMeasure[String], scoreConfig: SimilarityMeasureConfig[String, SimilarityMeasure[String]]): Double = {
        simMeasure.compare(subject1.get(attribute).head, subject2.get(attribute).head, scoreConfig.scale) * scoreConfig.weight
    }

    def expectedCompareStrategies: List[(List[String], List[String], SimilarityMeasureConfig[String, SimilarityMeasure[String]]) => Double] = List(
        CompareStrategy.caseInsensitiveCompare, CompareStrategy.singleStringCompare, CompareStrategy.defaultCompare
    )

    def testCompareInput: (List[String], List[String], SimilarityMeasureConfig[String, SimilarityMeasure[String]]) = {
        (
            List("very", "generic", "values"),
            List("even", "more", "values"),
            testConfig().head.scoreConfigs.head
        )
    }

    def cityBlockingScheme: ListBlockingScheme = {
        val scheme = new ListBlockingScheme()
        scheme.setAttributes("geo_city")
        scheme.tag = "GeoBlocking"
        scheme
    }

    def subjects: List[Subject] = List(
        Subject(id = UUID.fromString("974c4495-52fd-445c-b09b-8b769bfb4212"), master = null, datasource = datasource, name = Some("Volkswagen"), properties = Map("geo_city" -> List("Berlin", "Hamburg"), "geo_coords" -> List("52.1234;11.5677"), "geo_postal" -> List("1234"), "gen_income" -> List("1234"))),
        Subject(id = UUID.fromString("f2d98c16-2ac2-4cb7-bd65-4fcb17178060"), master = null, datasource = datasource, name = Some("Audi GmbH"), properties = Map("geo_city" -> List("Berlin"), "geo_coords" -> List("52.1234;13.1234"), "geo_postal" -> List("1256"), "gen_income" -> List("33"))),
        Subject(id = UUID.fromString("c9621786-1cce-45bf-968a-3e8d06e16fda"), master = null, datasource = datasource, name = Some("Porsche"), properties = Map("geo_coords" -> List("52.2345;13.6789"), "geo_postal" -> List("1904"), "gen_sectors" -> List("cars", "music", "games"))),
        Subject(id = UUID.fromString("0ef813fa-55d9-4712-a41d-46a810fca8c9"), master = null, datasource = datasource, name = Some("Ferrari"), properties = Map("gen_sectors" -> List("games", "cars", "music"), "geo_postal" -> List("2222")))
    )

    def subjectsStartingWithThe: List[Subject] = List(
        Subject(master = null, datasource = datasource, name = Some("The HPI")),
        Subject(master = null, datasource = datasource, name = Some("The University")),
        Subject(master = null, datasource = datasource, name = Some("Volkswagen"))
    )

    def stagings: List[Subject] = List(
        Subject(id = UUID.fromString("4fbc0340-4862-431f-9c28-a508234b8130"), master = null, datasource = datasource, name = Some("Volkswagen AG"), properties = Map("geo_city" -> List("Berlin", "New York"), "geo_postal" -> List("1414"), "gen_income" -> List("12"))),
        Subject(id = UUID.fromString("413c5711-67d0-1151-1077-b000000000b5"), master = null, datasource = datasource, name = Some("Audi GmbH"), properties = Map("geo_city" -> List("New York", "Hamburg"), "geo_coords" -> List("53;14"), "geo_postal" -> List("8888"), "gen_income" -> List("600")))
    )

    def goldStandard(subjects: List[Subject], stagings: List[Subject]): Set[(UUID, UUID)] = Set(
        (subjects.head.id, stagings.head.id),
        (subjects(1).id, stagings.last.id)
    )

    def testBlocks(sc: SparkContext): RDD[Block] = sc.parallelize(Seq(
        Block(key = "Volks", subjects = subjects.take(1), staging = stagings.take(1)),
        Block(key = "Audi ", subjects = List(subjects(1)), staging = List(stagings(1))),
        Block(key = "mix", subjects = subjects, staging = stagings.take(1))
    ))

    def flatSubjectBlocks(sc: SparkContext, subjects: List[Subject]): RDD[((String, String), Subject)] = {
        sc.parallelize(List(
            (("Berlin", "GeoBlocking"), subjects.head),
            (("Berlin", "GeoBlocking"), subjects(1)),
            (("Hamburg", "GeoBlocking"), subjects.head),
            (("undefined", "GeoBlocking"), subjects(2)),
            (("undefined", "GeoBlocking"), subjects(3)),
            (("volks", "SimpleBlocking"), subjects.head),
            (("audi ", "SimpleBlocking"), subjects(1)),
            (("porsc", "SimpleBlocking"), subjects(2)),
            (("ferra", "SimpleBlocking"), subjects(3))
        ))
    }

    def flatStagingBlocks(sc: SparkContext, staging: List[Subject]): RDD[((String, String), Subject)] = {
        sc.parallelize(List(
            (("Berlin", "GeoBlocking"), staging.head),
            (("Hamburg", "GeoBlocking"), staging.last),
            (("New York", "GeoBlocking"), staging.head),
            (("New York", "GeoBlocking"), staging.last),
            (("volks", "SimpleBlocking"), staging.head),
            (("audi ", "SimpleBlocking"), staging.last)
        ))
    }

    def flatBlocks(sc: SparkContext, subjects: List[Subject], staging: List[Subject]): RDD[((String, String), Subject)] = {
        sc.parallelize(List(
            (("Berlin", "GeoBlocking"), staging.head),
            (("Berlin", "GeoBlocking"), subjects.head),
            (("Berlin", "GeoBlocking"), subjects(1)),
            (("Hamburg", "GeoBlocking"), subjects.head),
            (("Hamburg", "GeoBlocking"), staging.last),
            (("New York", "GeoBlocking"), staging.head),
            (("New York", "GeoBlocking"), staging.last),
            (("undefined", "GeoBlocking"), subjects(2)),
            (("undefined", "GeoBlocking"), subjects(3)),
            (("Vol", "SimpleBlocking"), subjects.head),
            (("Vol", "SimpleBlocking"), staging.head),
            (("Aud", "SimpleBlocking"), subjects(1)),
            (("Aud", "SimpleBlocking"), staging.last),
            (("Por", "SimpleBlocking"), subjects(2)),
            (("Fer", "SimpleBlocking"), subjects(3))
        ))
    }

    def groupedBlocks(sc: SparkContext, subjects: List[Subject], stagings: List[Subject]): RDD[(String, Block)] = {
        sc.parallelize(List(
            ("GeoBlocking", Block(id = null, key = "Berlin", subjects = subjects.take(2), staging = stagings.take(1))),
            ("GeoBlocking", Block(id = null, key = "Hamburg", subjects = subjects.take(1), staging = List(stagings.last))),
            ("GeoBlocking", Block(id = null, key = "New York", staging = stagings)),
            ("GeoBlocking", Block(id = null, key = "undefined", subjects = subjects.slice(2, 4))),
            ("SimpleBlocking", Block(id = null, key = "volks", subjects = subjects.take(1), staging = stagings.take(1))),
            ("SimpleBlocking", Block(id = null, key = "audi ", subjects = List(subjects(1)), staging = List(stagings.last))),
            ("SimpleBlocking", Block(id = null, key = "porsc", subjects = List(subjects(2)))),
            ("SimpleBlocking", Block(id = null, key = "ferra", subjects = List(subjects(3))))
        ))
    }

    def groupedAndFilteredBlocks(sc: SparkContext, subjects: List[Subject], stagings: List[Subject]): RDD[(String, Block)] = {
        sc.parallelize(List(
            ("GeoBlocking", Block(id = null, key = "Berlin", subjects = subjects.take(2), staging = stagings.take(1))),
            ("GeoBlocking", Block(id = null, key = "Hamburg", subjects = subjects.take(1), staging = List(stagings.last))),
            ("GeoBlocking", Block(id = null, key = "New York", staging = stagings)),
            ("SimpleBlocking", Block(id = null, key = "volks", subjects = subjects.take(1), staging = stagings.take(1))),
            ("SimpleBlocking", Block(id = null, key = "audi ", subjects = List(subjects(1)), staging = List(stagings.last))),
            ("SimpleBlocking", Block(id = null, key = "porsc", subjects = List(subjects(2)))),
            ("SimpleBlocking", Block(id = null, key = "ferra", subjects = List(subjects(3))))
        ))
    }

    def filteredUndefinedBlockEvaluation(): Set[BlockEvaluation] = {
        Set(
            BlockEvaluation(
                null,
                "combined GeoBlocking, SimpleBlocking",
                Set(
                    BlockStats("Hamburg", 1, 1, 0.0),
                    BlockStats("Berlin", 2, 1, 0.5),
                    BlockStats("New York", 0, 2, 0.0),
                    BlockStats("audi ", 1, 1, 1.0),
                    BlockStats("volks", 1, 1, 1.0),
                    BlockStats("porsc", 1, 0, 0.0),
                    BlockStats("ferra", 1, 0, 0.0)
                ),
                Some("Blocking"), 1.0, 7, 5),
            BlockEvaluation(
                null,
                "GeoBlocking",
                Set(
                    BlockStats("New York", 0, 2, 0.0),
                    BlockStats("Hamburg", 1, 1, 0.0),
                    BlockStats("Berlin", 2, 1, 0.5)
                ),
                Some("Blocking"), 0.5, 3, 3),
            BlockEvaluation(
                null,
                "SimpleBlocking",
                Set(
                    BlockStats("audi ", 1, 1, 1.0),
                    BlockStats("ferra", 1, 0, 0.0),
                    BlockStats("porsc", 1, 0, 0.0),
                    BlockStats("volks", 1, 1, 1.0)
                ),
                Some("Blocking"), 1.0, 4, 2)
        )
    }

    def blockEvaluationTestSubjects(): (List[Subject], List[Subject]) = {
        val subjects = List.fill(101)(Subject(UUID.randomUUID, null, datasource, Option("Haushund"))) ++
            List.fill(5)(Subject(UUID.randomUUID, null, datasource, Option("Nicht Haushund")))
        val staging = List.fill(101)(Subject(UUID.randomUUID, null, datasource, Option("Haushund")))
        (subjects, staging)
    }

    def filteredSmallBlockEvaluation(): Set[BlockEvaluation] = {
        Set(
            BlockEvaluation(
                null,
                "combined SimpleBlocking",
                Set(BlockStats("haush", 101, 101, 0.0)),
                Some("Blocking"), 0.0, 1, 10201),
            BlockEvaluation(
                null,
                "SimpleBlocking",
                Set(BlockStats("haush", 101, 101, 0.0)),
                Some("Blocking"), 0.0, 1, 10201)
        )
    }

    def blockEvaluationWithComment(): Set[BlockEvaluation] = {
        Set(
            BlockEvaluation(
                null,
                "combined GeoBlocking, SimpleBlocking",
                Set(
                    BlockStats("Hamburg", 1, 1, 0.0),
                    BlockStats("undefined", 2, 0, 0.0),
                    BlockStats("Berlin", 2, 1, 0.5),
                    BlockStats("New York", 0, 2, 0.0),
                    BlockStats("audi ", 1, 1, 1.0),
                    BlockStats("volks", 1, 1, 1.0),
                    BlockStats("porsc", 1, 0, 0.0),
                    BlockStats("ferra", 1, 0, 0.0)
                ),
                Some("Test comment"), 1.0, 8, 5),
            BlockEvaluation(
                null,
                "GeoBlocking",
                Set(
                    BlockStats("New York", 0, 2, 0.0),
                    BlockStats("Hamburg", 1, 1, 0.0),
                    BlockStats("Berlin", 2, 1, 0.5),
                    BlockStats("undefined", 2, 0, 0.0)
                ),
                Some("Test comment"), 0.5, 4, 3),
            BlockEvaluation(
                null,
                "SimpleBlocking",
                Set(
                    BlockStats("audi ", 1, 1, 1.0),
                    BlockStats("ferra", 1, 0, 0.0),
                    BlockStats("porsc", 1, 0, 0.0),
                    BlockStats("volks", 1, 1, 1.0)
                ),
                Some("Test comment"), 1.0, 4, 2)
        )
    }

    def blockEvaluationWithOutPC(): Set[BlockEvaluation] = {
        Set(
            BlockEvaluation(
                null,
                "combined GeoBlocking, SimpleBlocking",
                Set(
                    BlockStats("Hamburg", 1, 1, 0.0),
                    BlockStats("undefined", 2, 0, 0.0),
                    BlockStats("Berlin", 2, 1, 0.5),
                    BlockStats("New York", 0, 2, 0.0),
                    BlockStats("audi ", 1, 1, 1.0),
                    BlockStats("volks", 1, 1, 1.0),
                    BlockStats("porsc", 1, 0, 0.0),
                    BlockStats("ferra", 1, 0, 0.0)
                ),
                Some("Test comment"), 0.0, 8, 5),
            BlockEvaluation(
                null,
                "GeoBlocking",
                Set(
                    BlockStats("New York", 0, 2, 0.0),
                    BlockStats("Hamburg", 1, 1, 0.0),
                    BlockStats("Berlin", 2, 1, 0.5),
                    BlockStats("undefined", 2, 0, 0.0)
                ),
                Some("Test comment"), 0.0, 4, 3),
            BlockEvaluation(
                null,
                "SimpleBlocking",
                Set(
                    BlockStats("audi ", 1, 1, 1.0),
                    BlockStats("ferra", 1, 0, 0.0),
                    BlockStats("porsc", 1, 0, 0.0),
                    BlockStats("volks", 1, 1, 1.0)
                ),
                Some("Test comment"), 0.0, 4, 2)
        )
    }

    def emptyBlockEvaluation(): Set[BlockEvaluation] = {
        Set(
            BlockEvaluation(null, "combined GeoBlocking, SimpleBlocking", Set(), Some("Blocking"), 1.0, 4, 5),
            BlockEvaluation(null, "GeoBlocking", Set(), Some("Blocking"), 0.5, 2, 3),
            BlockEvaluation(null, "SimpleBlocking", Set(), Some("Blocking"), 1.0, 2, 2)
        )
    }

    def emptyBlockEvaluationWithComment(): Set[BlockEvaluation] = {
        Set(
            BlockEvaluation(null, "combined GeoBlocking, SimpleBlocking", Set(), Some("Test comment"), 1.0, 4, 5),
            BlockEvaluation(null, "GeoBlocking", Set(), Some("Test comment"), 0.5, 2, 3),
            BlockEvaluation(null, "SimpleBlocking", Set(), Some("Test comment"), 1.0, 2, 2)
        )
    }

    def simpleBlockingScheme: List[List[String]] = List(List("audi "), List("volks"), List("ferra"), List("porsc"))
    def simpleBlockingSchemeWithThe: List[List[String]] = List(List("hpi"), List("unive"), List("volks"))
    def lastLettersBlockingScheme: List[List[String]] = List(List(" gmbh"), List("wagen"), List("rrari"), List("rsche"), List("undefined"))
    def listBlockingScheme: List[List[String]] = List(List("Berlin", "Hamburg", "1234"), List("Berlin", "33"), List("undefined"))
    def geoCoordsBlockingSchemeDefault: List[List[String]] = List(
        List("52.1;11.5", "52.1;11.6", "52.2;11.5", "52.2;11.6"),
        List("52.1;13.1", "52.1;13.2", "52.2;13.1", "52.2;13.2"),
        List("52.2;13.6", "52.2;13.7", "52.3;13.6", "52.3;13.7"),
        List("undefined")
    )
    def geoCoordsBlockingSchemeDecimals: List[List[String]] = List(
        List("52.12;11.56", "52.12;11.57", "52.13;11.56", "52.13;11.57"),
        List("52.12;13.12", "52.12;13.13", "52.13;13.12", "52.13;13.13"),
        List("52.23;13.67", "52.23;13.68", "52.24;13.67", "52.24;13.68"),
        List("undefined")
    )
    def randomBlockingScheme: List[List[String]] = List(List("-235"), List("754"), List("446"), List("-7"))
    def mapBlockingScheme: List[List[String]] = List(List("Vol"), List("Vol"), List("Aud"), List("Aud"), List("Por"), List("Fer"))

    def features(sc: SparkContext): RDD[FeatureEntry] = sc.parallelize(List(
        FeatureEntry(subject = Subject(id = idList.head, master = null, datasource = datasource), staging = Subject(id = idList(1), master = null, datasource = datasource)),
        FeatureEntry(subject = Subject(id = idList(2), master = null, datasource = datasource), staging = Subject(id = idList(3), master = null, datasource = datasource)),
        FeatureEntry(subject = Subject(id = idList(4), master = null, datasource = datasource), staging = Subject(id = idList(5), master = null, datasource = datasource))
    ))

    def goldStandard(sc: SparkContext): RDD[(UUID, UUID)] = sc.parallelize(List(
        (idList.head, idList(1)), (idList(2), idList(3)), (idList(5), idList(6))
    ))

    def labeledFeatures(sc: SparkContext): RDD[FeatureEntry] = sc.parallelize(List(
        FeatureEntry(subject = Subject(id = idList.head, master = null, datasource = datasource), staging = Subject(id = idList(1), name = Option("Audi AG"), master = null, datasource = datasource), correct = true),
        FeatureEntry(subject = Subject(id = idList(2), master = null, datasource = datasource), staging = Subject(id = idList(3), master = null, datasource = datasource, name = Option("Volkswagen AG")), correct = true),
        FeatureEntry(subject = Subject(id = idList(4), master = null, datasource = datasource), staging = Subject(id = idList(5), master = null, datasource = datasource, name = Option("Porsche AG")))
    ))

    def requiredSettings: List[String] = {
        List("keyspaceSubjectTable", "subjectTable", "keyspaceStagingTable", "stagingTable", "keyspaceStatsTable", "statsTable")
    }


    def distinctDuplicateCandidates(sc: SparkContext): RDD[(Subject, Subject, Double)] = {
        sc.parallelize(List(
            (subjects.head, stagings.head, 0.0),
            (subjects(1), stagings(1), 0.0),
            (subjects(1), stagings.head, 0.0),
            (subjects(2), stagings.head, 0.0),
            (subjects(3), stagings.head, 0.0)
        ))
    }

    def featureEntries: List[FeatureEntry] = {
        List(
            FeatureEntry(null, null, null, Map("a" -> List(0.2, 0.4, 0.1)), false),
            FeatureEntry(null, null, null, Map("a" -> List(0.1, 0.3, 0.2)), false),
            FeatureEntry(null, null, null, Map("a" -> List(0.3, 0.5, 0.15)), false),
            FeatureEntry(null, null, null, Map("a" -> List(0.4, 0.3, 0.1)), false),
            FeatureEntry(null, null, null, Map("a" -> List(0.345, 0.6, 0.3)), false),
            FeatureEntry(null, null, null, Map("a" -> List(0.6, 0.7, 0.5)), true),
            FeatureEntry(null, null, null, Map("a" -> List(0.124, 0.4, 0.3)), false),
            FeatureEntry(null, null, null, Map("a" -> List(0.78, 0.9, 0.5)), true),
            FeatureEntry(null, null, null, Map("a" -> List(0.9, 0.7, 0.6)), true),
            FeatureEntry(null, null, null, Map("a" -> List(0.85, 0.6, 0.3)), true))
    }

    def dbpediaEntries: List[Subject] = {
        List(
            Subject(id = idList.head, master = null, datasource = datasource, name = Option("Audi ")),
            Subject(id = idList(2), master = null, datasource = datasource, name = Option("Volkswagen")),
            Subject(id = idList(4), master = null, datasource = datasource, name = Option("Porsche")))
    }

    def wikidataEntries: List[Subject] = {
        List(
            Subject(id = idList(1), master = null, datasource = datasource, name = Option("Audi AG")),
            Subject(id = idList(3), master = null, datasource = datasource, name = Option("Volkswagen AG")),
            Subject(id = idList(5), master = null, datasource = datasource, name = Option("Porsche AG")))
    }

    def blockSplitSubjects(): List[Subject] = {
        List(
            Subject.master(idList.head).copy(name = Option("Audi 1")),
            Subject.master(idList(1)).copy(name = Option("Audi 2")),
            Subject.master(idList(2)).copy(name = Option("Audi 3")),
            Subject.master(idList(3)).copy(name = Option("Audi 4")),
            Subject.master(idList(4)).copy(name = Option("Audi 5")),
            Subject.master(idList(5)).copy(name = Option("Audi 6")),
            Subject.master(idList(6)).copy(name = Option("Audi 7")),
            Subject.master(idList(7)).copy(name = Option("Porsche 1"))
        )
    }

    def blockSplitStagedSubjects(): List[Subject] = {
        List(
            Subject.master(idList(7)).copy(name = Option("Audi 8")),
            Subject.master(idList(6)).copy(name = Option("Audi 9")),
            Subject.master(idList(5)).copy(name = Option("Audi 10")),
            Subject.master(idList(4)).copy(name = Option("Audi 11")),
            Subject.master(idList(3)).copy(name = Option("Audi 12")),
            Subject.master(idList(2)).copy(name = Option("Audi 13")),
            Subject.master(idList(1)).copy(name = Option("Audi 14")),
            Subject.master(idList.head).copy(name = Option("Porsche 2"))
        )
    }

    def splitBlocks(): Set[Block] = {
        Set(
            Block(
                null,
                "porsc",
                List(Subject.master(idList(7)).copy(name = Option("Porsche 1"))),
                List(Subject.master(idList.head).copy(name = Option("Porsche 2")))
            ),
            Block(
                null,
                "audi _0",
                List(
                    Subject.master(idList.head).copy(name = Option("Audi 1")),
                    Subject.master(idList(1)).copy(name = Option("Audi 2")),
                    Subject.master(idList(2)).copy(name = Option("Audi 3"))
                ),
                List(
                    Subject.master(idList(7)).copy(name = Option("Audi 8")),
                    Subject.master(idList(6)).copy(name = Option("Audi 9")),
                    Subject.master(idList(5)).copy(name = Option("Audi 10")),
                    Subject.master(idList(4)).copy(name = Option("Audi 11")),
                    Subject.master(idList(3)).copy(name = Option("Audi 12")),
                    Subject.master(idList(2)).copy(name = Option("Audi 13")),
                    Subject.master(idList(1)).copy(name = Option("Audi 14"))
                )
            ),
            Block(
                null,
                "audi _1",
                List(
                    Subject.master(idList(3)).copy(name = Option("Audi 4")),
                    Subject.master(idList(4)).copy(name = Option("Audi 5")),
                    Subject.master(idList(5)).copy(name = Option("Audi 6")),
                    Subject.master(idList(6)).copy(name = Option("Audi 7"))
                ),
                List(
                    Subject.master(idList(7)).copy(name = Option("Audi 8")),
                    Subject.master(idList(6)).copy(name = Option("Audi 9")),
                    Subject.master(idList(5)).copy(name = Option("Audi 10")),
                    Subject.master(idList(4)).copy(name = Option("Audi 11")),
                    Subject.master(idList(3)).copy(name = Option("Audi 12")),
                    Subject.master(idList(2)).copy(name = Option("Audi 13")),
                    Subject.master(idList(1)).copy(name = Option("Audi 14"))
                )
            )
        )
    }

    def blocksWithSubsetBlocks(tag: String): List[(String, Block)] = {
        List(
            Block(null, "audi", List.fill(5)(Subject.master()), List.fill(4)(Subject.master())),
            Block(null, "audi ", List.fill(5)(Subject.master()), List.fill(4)(Subject.master())),
            Block(null, "au", List.fill(3)(Subject.master()), List.fill(2)(Subject.master())),
            Block(null, "bmw", List.fill(1)(Subject.master()), List.fill(10)(Subject.master())),
            Block(null, "bmw g", List.fill(2)(Subject.master()), List.fill(20)(Subject.master())),
            Block(null, "volks", List.fill(4)(Subject.master()), List.fill(7)(Subject.master())),
            Block(null, "vw", List.fill(6)(Subject.master()), List.fill(8)(Subject.master()))
        ).map((tag, _))
    }

    def blocksWithSubsetBlocksAndTags(tag1: String, tag2: String): List[(String, Block)] = {
        List(
            (tag1, Block(null, "audi", List.fill(5)(Subject.master()), List.fill(4)(Subject.master()))),
            (tag1, Block(null, "audi ", List.fill(5)(Subject.master()), List.fill(4)(Subject.master()))),
            (tag1, Block(null, "au", List.fill(3)(Subject.master()), List.fill(2)(Subject.master()))),
            (tag1, Block(null, "bmw", List.fill(1)(Subject.master()), List.fill(10)(Subject.master()))),
            (tag1, Block(null, "bmw g", List.fill(2)(Subject.master()), List.fill(20)(Subject.master()))),
            (tag1, Block(null, "volks", List.fill(4)(Subject.master()), List.fill(7)(Subject.master()))),
            (tag1, Block(null, "vw", List.fill(6)(Subject.master()), List.fill(8)(Subject.master()))),
            (tag2, Block(null, "audi", List.fill(5)(Subject.master()), List.fill(4)(Subject.master()))),
            (tag2, Block(null, "audi ", List.fill(5)(Subject.master()), List.fill(4)(Subject.master()))),
            (tag2, Block(null, "au", List.fill(3)(Subject.master()), List.fill(2)(Subject.master()))),
            (tag2, Block(null, "bmw", List.fill(1)(Subject.master()), List.fill(10)(Subject.master()))),
            (tag2, Block(null, "bmw g", List.fill(2)(Subject.master()), List.fill(20)(Subject.master()))),
            (tag2, Block(null, "volks", List.fill(4)(Subject.master()), List.fill(7)(Subject.master()))),
            (tag2, Block(null, "vw", List.fill(6)(Subject.master()), List.fill(8)(Subject.master())))
        )
    }

    def subjectsForEmptyBlocks(): (List[Subject], List[Subject]) = {
        val subjects = List(
            Subject.master().copy(name = Option("Audi AG")),
            Subject.master().copy(name = Option("AUDI AG"))
        )
        val stagedSubjects = List(
            Subject.master().copy(name = Option("Audi"))
        )
        (subjects, stagedSubjects)
    }
}
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
// scalastyle:on file.size.limit
