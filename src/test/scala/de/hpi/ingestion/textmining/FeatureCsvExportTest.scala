package de.hpi.ingestion.textmining

import de.hpi.ingestion.implicits.CollectionImplicits._
import org.scalatest.{FlatSpec, Matchers}
import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.textmining.preprocessing.FeatureCsvExport

class FeatureCsvExportTest extends FlatSpec with SharedSparkContext with Matchers {
	"Features csv string" should "be exactly this string" in {
		val input = List(sc.parallelize(TestData.featureEntriesList())).toAnyRDD()
		val featuresCsvString = FeatureCsvExport.run(input, sc)
			.fromAnyRDD[String]()
			.head
			.collect
			.head
		featuresCsvString shouldEqual TestData.featuresCsvString()
	}
}
