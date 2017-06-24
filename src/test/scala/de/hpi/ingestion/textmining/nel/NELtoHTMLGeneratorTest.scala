package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.implicits.CollectionImplicits._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class NELtoHTMLGeneratorTest extends FlatSpec with SharedSparkContext with Matchers {
	"Articles with links" should "not be empty" in {
		val articles = sc.parallelize(TestData.aliasSearchArticles())
		val input = List(articles).toAnyRDD()
		val articlesWithLinks = NELtoHTMLGenerator.run(input, sc)
		articlesWithLinks should not be empty
	}
}
