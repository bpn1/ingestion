import DataLake.Subject
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec

class DBpediaDeduplicationUnitTest extends FlatSpec with SharedSparkContext with RDDComparisons {
	"keyBySingleProperty" should "key a rdd by a single value of a property" in {
		val subjectList = TestData.subjectList()
		val subjects = sc.parallelize(subjectList)
		val subjectsName = DBPediaDeduplication.keyBySingleProperty(subjects, "wikipedia_name")
		val expected = TestData.singleKeyRDD(sc, subjectList)
		assertRDDEquals(expected, subjectsName)
	}

	"keyByProperty" should "key a rdd by all values of a property" in {
		val subjectList = TestData.dbpediaSubjectList()
		val subjects = sc.parallelize(subjectList)
		val subjectsSameAs = DBPediaDeduplication.keyByProperty(subjects, "owl:sameAs")
		val expected = TestData.multiKeyRDD(sc, subjectList)
		assertRDDEquals(expected, subjectsSameAs)
	}

	"joinOnName" should "join to subjectRDDs on the name" in {
		val dbpediaList = TestData.dbpediaSubjectList()
		val dbpediaRDD = sc.parallelize(dbpediaList)
		val subjectList = TestData.subjectList()
		val subjectRDD = sc.parallelize(subjectList)
		val joined = DBPediaDeduplication.joinOnName(dbpediaRDD, subjectRDD)
		val expected = TestData.nameJoinedRDD(sc, dbpediaList, subjectList)
		assertRDDEquals(expected, joined)
	}

	"joinOnWikiId" should "join to subjectRDDs on the id" in {
		val dbpediaList = TestData.dbpediaSubjectList()
		val dbpediaRDD = sc.parallelize(dbpediaList)
		val subjectList = TestData.subjectList()
		val subjectRDD = sc.parallelize(subjectList)
		val joined = DBPediaDeduplication.joinOnWikiId(dbpediaRDD, subjectRDD)
		val expected = TestData.idJoinedRDD(sc, dbpediaList, subjectList)
		assertRDDEquals(expected, joined)
	}
}