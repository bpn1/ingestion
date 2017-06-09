package de.hpi.ingestion.dataimport.spiegel

import com.holdenkarau.spark.testing.SharedSparkContext
import de.hpi.ingestion.dataimport.spiegel.models.SpiegelArticle
import org.scalatest.{FlatSpec, Matchers}
import de.hpi.ingestion.implicits.CollectionImplicits._

class SpiegelImportTest extends FlatSpec with Matchers with SharedSparkContext {

	"Spiegel articles" should "be parsed" in {
		val input = List(sc.parallelize(TestData.spiegelFile())).toAnyRDD()
		val articles = SpiegelImport.run(input, sc).fromAnyRDD[SpiegelArticle]().head.collect.toSet
		val expectedArticles = TestData.parsedArticles().toSet
		articles shouldEqual expectedArticles
	}

	"Article values" should "be extracted" in {
		val articles = TestData.spiegelJson().map(SpiegelImport.fillEntityValues)
		val expectedArticles = TestData.parsedArticles()
		articles shouldEqual expectedArticles
	}

}
