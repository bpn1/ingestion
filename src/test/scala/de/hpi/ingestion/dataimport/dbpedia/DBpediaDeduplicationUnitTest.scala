package de.hpi.ingestion.dataimport.dbpedia

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.{FlatSpec, Matchers}

class DBpediaDeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons with Matchers {
	"keyBySingleProperty" should "key subjects by a single value of a given property" in {
		val subjects = TestData.subjectList()
		val subjectRDD = sc.parallelize(subjects)
		val subjectNames = DBPediaDeduplication.keyBySingleProperty(subjectRDD, "wikipedia_name")
		val expected = TestData.singleKeyRDD(sc, subjects)
		assertRDDEquals(expected, subjectNames)
	}

	"keyByProperty" should "key subjects by a list of values of a given property" in {
		val subjects = TestData.dbpediaSubjectList()
		val subjectRDD = sc.parallelize(subjects)
		val subjectSameAs = DBPediaDeduplication.keyByProperty(subjectRDD, "owl:sameAs")
		val expected = TestData.multiKeyRDD(sc, subjects)
		assertRDDEquals(expected, subjectSameAs)
	}

	"joinOnName" should "join two subject RDDs on the wikipedia_name" in {
		val subjects = TestData.subjectList()
		val dbpedia = TestData.dbpediaSubjectList()
		val subjectRDD = sc.parallelize(subjects)
		val dbpediaRDD = sc.parallelize(dbpedia)
		val joined = DBPediaDeduplication.joinOnName(dbpediaRDD, subjectRDD)
		val expected = TestData.nameJoinedRDD(sc, dbpedia, subjects)
		assertRDDEquals(expected, joined)
	}

	"joinOnWikiId" should "join two subject RDDs on the wikidata_id" in {
		val subjects = TestData.subjectList()
		val dbpedia = TestData.dbpediaSubjectList()
		val subjectRDD = sc.parallelize(subjects)
		val dbpediaRDD = sc.parallelize(dbpedia)
		val joined = DBPediaDeduplication.joinOnWikiId(dbpediaRDD, subjectRDD)
		val expected = TestData.idJoinedRDD(sc, dbpedia, subjects)
		assertRDDEquals(expected, joined)
	}

	"joinGoldStandard" should "join two duplicate RDDs to a subject RDD containing all duplicate subjects" in {
		val subjects = TestData.subjectList()
		val dbpedia = TestData.dbpediaSubjectList()
		val nameJoined = TestData.nameJoinedRDD(sc, dbpedia, subjects)
		val idJoined = TestData.idJoinedRDD(sc, dbpedia, subjects)
		val joined = DBPediaDeduplication.joinGoldStandard(nameJoined, idJoined)
		val expected = TestData.joinedSubjectRDD(sc, dbpedia, subjects)
		assertRDDEquals(expected, joined)
	}

	"joinCandidates" should "join two candidate RDDs" in {
		val subjects = TestData.subjectList()
		val dbpedia = TestData.dbpediaSubjectList()
		val nameCandidateRDD = TestData.nameCandidates(sc, dbpedia, subjects)
		val idCandidateRDD = TestData.idCandidates(sc, dbpedia, subjects)
		val joinedCandidateRDD = DBPediaDeduplication.joinCandidates(nameCandidateRDD, idCandidateRDD)
		val expected = TestData.candidatesJoinedRDD(sc, dbpedia, subjects)
		assertRDDEquals(expected, joinedCandidateRDD)
	}
}
