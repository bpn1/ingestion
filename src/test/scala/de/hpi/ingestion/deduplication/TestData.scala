package de.hpi.ingestion.deduplication

import java.util.UUID
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TestData {
	val idList = List.fill(9)(UUID.randomUUID())
	def testData(sc: SparkContext): RDD[((UUID, UUID), Double)] = {
		sc.parallelize(Seq(
			((idList.head, idList(1)), 1:Double),
			((idList(2), idList(3)), 1:Double),
			((idList(4), idList(5)), 1:Double),
			((idList(6), idList(7)), 1:Double)
		))
	}

	def trainigsData(sc: SparkContext): RDD[((UUID, UUID), Double)] = {
		sc.parallelize(Seq(
			((idList.head, idList(1)), 0.7),
			((idList(2), idList(3)), 0.8),
			((idList(4), idList(7)), 0.5),
			((idList(6), idList(9)), 0.6)
		))
	}

	def truePositives(sc: SparkContext): RDD[(Double, Double)] = {
		sc.parallelize(List((0.7, 1:Double), (0.8, 1:Double)))
	}
	def falsePositives(sc: SparkContext): RDD[(Double, Double)] = {
		sc.parallelize(List((0.5, 0:Double), (0.6, 0:Double)))
	}
	def falseNegatives(sc: SparkContext): RDD[(Double, Double)] = {
		sc.parallelize(List.fill(2)((0:Double, 1:Double)))
	}
}
