package DataLake

import java.util.UUID

import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD

class DeduplicationRDDTest extends FlatSpec with SharedSparkContext with RDDComparisons {
	"generateBlocks" should "generate a block for each industry" in {
		val subjects = SubjectRDD()
		val blocks = Deduplication.generateBlocks(subjects, sc)
		val expected = SubjectBlockRDD()
		assertRDDEquals(expected, blocks)
	}

	it should "merge the blocks if two subject RDDs are given" in {
		val subjects = SubjectRDD()
		val stagings = StagingSubjectsRDD()
		// Conversion from list to set because of an unordered comparison
		val blocks = Deduplication.generateBlocks(subjects, stagings, sc).map(x => (x._1, x._2.toSet))
		val expected = SubjectAndStagingBlockRDD().map(x => (x._1, x._2.toSet))
		assertRDDEquals(expected, blocks)
	}

	"collectIndustries" should "collect all industries of the subjects" in {
		val subjects = SubjectRDD()
		val industries = Deduplication.collectIndustries(subjects).toSet
		val expected = Set("auto", "kohle", "uncategorized", "stahl")
		assert(expected, industries)
	}

	"findDuplicates" should "find all duplicates having a similarity score above the threshold" in {
		val config = Deduplication.parseConfig("./src/test/resources/config.xml")
		val blocks = DublicateCandidatesRDD()
		val duplicates = Deduplication.findDuplicates(blocks, config).map(x => (x._1, x._2.toSet))
		val expected = DublicatesRDD(config).map(x => (x._1, x._2.toSet))
		assertRDDEquals(expected, duplicates)
	}

	val audi = Subject(id = UUID.randomUUID(), name = Option("Audi"), properties = Map(("branche", List("auto", "kohle"))))
	val bmw = Subject(id = UUID.randomUUID(), name = Option("BMW"), properties = Map(("branche", List("auto", "kohle"))))
	val lamborghini = Subject(id = UUID.randomUUID(), name = Option("Lamborghini"), properties = Map(("branche", List("auto", "metall"))))
	val opel = Subject(id = UUID.randomUUID(), name = Option("Opel"), properties = Map(("branche", List("auto", "metall"))))
	val porsche = Subject(id = UUID.randomUUID(), name = Option("Porsche"))
	val vw = Subject(id = UUID.randomUUID(), name = Option("VW"), properties = Map(("branche", List("auto", "stahl"))))

	// duplicates
	val audi2 = Subject(id = UUID.randomUUID(), name = Option("Audi AG"), properties = Map(("branche", List("auto", "kohle"))))
	val bmw2 = Subject(id = UUID.randomUUID(), name = Option("Bmw"), properties = Map(("branche", List("auto", "kohle"))))

	def SubjectRDD(): RDD[Subject] = {
		sc.parallelize(Seq(audi, bmw, porsche, vw))
	}

	def StagingSubjectsRDD(): RDD[Subject] = {
		sc.parallelize(Seq(opel, lamborghini, audi))
	}

	def SubjectBlockRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			("auto", Iterable(audi, bmw, vw)),
			("kohle", Iterable(audi, bmw)),
			("uncategorized", Iterable(porsche)),
			("stahl", Iterable(vw))
		))
	}

	def SubjectAndStagingBlockRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			("auto", Iterable(audi, bmw, lamborghini, opel, vw)),
			("kohle", Iterable(audi, bmw)),
			("metall", Iterable(lamborghini, opel)),
			("uncategorized", Iterable(porsche)),
			("stahl", Iterable(vw))
		))
	}

	def DublicateCandidatesRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			("auto", Iterable(audi, audi2, lamborghini, opel, vw)),
			("kohle", Iterable(audi, bmw, bmw2))
		))
	}

	def DublicatesRDD(config: List[scoreConfig[_,_ <: SimilarityMeasure[_]]]): RDD[(String, Iterable[possibleDuplicate])] = {
		sc.parallelize(Seq(
			("auto", Iterable(possibleDuplicate(audi.id, audi2.id, Deduplication.compare(audi, audi2, config)))),
			("kohle", Iterable(possibleDuplicate(bmw.id, bmw2.id, Deduplication.compare(bmw, bmw2, config))))))
	}
}