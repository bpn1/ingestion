package DataLake

import java.util.UUID

import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD

class DeduplicationRDDTest extends FlatSpec with SharedSparkContext with RDDComparisons {
	"generateBlocks" should "generate a block for each industry" in {
		val subjects = subjectRDD()
		val blocks = Deduplication.generateBlocks(subjects, sc)
		val expected = subjectBlockRDD()
		assertRDDEquals(expected, blocks)
	}

	it should "merge the blocks if two subject RDDs are given" in {
		val subjects = subjectRDD()
		val stagings = stagingSubjectsRDD()
		// Conversion from list to set because of an unordered comparison
		val blocks = Deduplication.generateBlocks(subjects, stagings, sc)
			.map(x => (x._1, x._2.toSet))
		val expected = subjectAndStagingBlockRDD()
			.map(x => (x._1, x._2.toSet))
		assertRDDEquals(expected, blocks)
	}

	"collectIndustries" should "collect all industries of the subjects" in {
		val subjects = subjectRDD()
		val industries = Deduplication.collectIndustries(subjects).toSet
		val expected = Set("auto", "kohle", "uncategorized", "stahl")
		assert(expected, industries)
	}

	"findDuplicates" should "find all duplicates having a similarity score above the threshold" in {
		val config = Deduplication.parseConfig("./src/test/resources/config.xml")
		val blocks = duplicateCandidatesRDD()
		val duplicates = Deduplication.findDuplicates(blocks, config)
			.map(x => (x._1, x._2.toSet))
		val expected = duplicatesRDD(config)
			.map(x => (x._1, x._2.toSet))
		assertRDDEquals(expected, duplicates)
	}

	def testSubject(subjectName: String, props: Map[String, List[String]]): Subject = {
		Subject(id = UUID.randomUUID(), name = Option(subjectName), properties = props)
	}

	val audi = testSubject("Audi", Map(("branche", List("auto", "kohle"))))
	val bmw = testSubject("BMW", Map(("branche", List("auto", "kohle"))))
	val lamborghini = testSubject("Lamborghini", Map(("branche", List("auto", "metall"))))
	val opel = testSubject("Opel", Map(("branche", List("auto", "metall"))))
	val porsche = Subject(id = UUID.randomUUID(), name = Option("Porsche"))
	val vw = testSubject("VW", Map(("branche", List("auto", "stahl"))))

	// duplicates
	val audi2 = testSubject("Audi AG", Map(("branche", List("auto", "kohle"))))
	val bmw2 = testSubject("Bmw", Map(("branche", List("auto", "kohle"))))

	def subjectRDD(): RDD[Subject] = {
		sc.parallelize(Seq(audi, bmw, porsche, vw))
	}

	def stagingSubjectsRDD(): RDD[Subject] = {
		sc.parallelize(Seq(opel, lamborghini, audi))
	}

	def subjectBlockRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			("auto", Iterable(audi, bmw, vw)),
			("kohle", Iterable(audi, bmw)),
			("uncategorized", Iterable(porsche)),
			("stahl", Iterable(vw))
		))
	}

	def subjectAndStagingBlockRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			("auto", Iterable(audi, bmw, lamborghini, opel, vw)),
			("kohle", Iterable(audi, bmw)),
			("metall", Iterable(lamborghini, opel)),
			("uncategorized", Iterable(porsche)),
			("stahl", Iterable(vw))
		))
	}

	def duplicateCandidatesRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			("auto", Iterable(audi, audi2, lamborghini, opel, vw)),
			("kohle", Iterable(audi, bmw, bmw2))
		))
	}

	def duplicatesRDD(
		config: List[scoreConfig[_,_ <: SimilarityMeasure[_]]]
	): RDD[(String, Iterable[possibleDuplicate])] =
	{
		sc.parallelize(Seq(
			("auto", Iterable(
				possibleDuplicate(audi.id, audi2.id, Deduplication.compare(audi, audi2, config)))),
			("kohle", Iterable(
				possibleDuplicate(bmw.id, bmw2.id, Deduplication.compare(bmw, bmw2, config))))))
	}
}
