package DataLake

import java.util.UUID

import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD

class DeduplicationRDDTest extends FlatSpec with SharedSparkContext with RDDComparisons {
	"generateBlocks" should "generate a block for each industry" in {
		val subjects = SubjectRDD()
		val blocks = Deduplication.generateBlocks(subjects)
		val expected = SubjectBlockRDD()
		assertRDDEquals(expected, blocks)
	}

	it should "merge the blocks if two subject RDDs are given" in {
		val subjects = SubjectRDD()
		val stagings = StagingSubjectsRDD()
		val blocks = Deduplication.generateBlocks(subjects, stagings)
		val expected = SubjectAndStagingRDD()
		blocks.collect.foreach(println)
		expected.collect.foreach(println)
		assertRDDEquals(expected, blocks)
	}

	val audi = Subject(id = UUID.randomUUID(), name = Option("Audi"), properties = Map(("branche", List("auto"))))
	val bmw = Subject(id = UUID.randomUUID(), name = Option("BMW"), properties = Map(("branche", List("auto"))))
	val lamborghini = Subject(id = UUID.randomUUID(), name = Option("Lamborghini"), properties = Map(("branche", List("auto"))))
	val opel = Subject(id = UUID.randomUUID(), name = Option("Opel"), properties = Map(("branche", List("auto"))))
	val porsche = Subject(id = UUID.randomUUID(), name = Option("Porsche"))
	val vw = Subject(id = UUID.randomUUID(), name = Option("VW"), properties = Map(("branche", List("auto"))))

	def SubjectRDD(): RDD[Subject] = {
		sc.parallelize(Seq(audi, bmw, porsche, vw))
	}

	def StagingSubjectsRDD(): RDD[Subject] = {
		sc.parallelize(Seq(opel, lamborghini, audi))
	}

	def SubjectBlockRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			Tuple2("auto", Iterable(audi, bmw, vw)),
			Tuple2("uncategorized", Iterable(porsche))
		))
	}

	def SubjectAndStagingRDD(): RDD[(String, Iterable[Subject])] = {
		sc.parallelize(Seq(
			Tuple2("auto", Iterable(audi, bmw, lamborghini, opel, vw)),
			Tuple2("uncategorized", Iterable(porsche))
		))
	}
}