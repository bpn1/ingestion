import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

class WikipediaAliasCounterTest extends FlatSpec with SharedSparkContext {
	"Counted aliases" should "keep the right alias name" in {
		cleanedGroupedAliasesTestRDD()
			.map(link => (WikipediaAliasCounter.countAliasOccurrences(link.alias), link.alias))
			.collect
			.foreach { case (aliasCounter, originalAlias) => assert(aliasCounter.alias == originalAlias) }
	}

	"Alias occurrences" should "be correct identified as link or no link" in {
		val allAliases = allAliasesTestRDD()
		val aliasOccurrencesInArticles = parsedWikipediaTestRDD()
			.map(article => WikipediaAliasCounter.identifyAliasOccurrencesInArticle(article, allAliases))
		assert(areRDDsEqual(aliasOccurrencesInArticles.asInstanceOf[RDD[Any]], aliasOccurrencesInArticlesTestRDD().asInstanceOf[RDD[Any]]))
	}

	def areRDDsEqual(rdd1: RDD[Any], rdd2: RDD[Any]): Boolean = {
		//		rdd1
		//			.collect
		//			.foreach(println)
		//		println("--------")
		//		rdd2
		//			.collect
		//			.foreach(println)

		val size1 = rdd1.count
		val size2 = rdd2.count
		if (size1 != size2)
			return false
		val intersectionCount = rdd1.intersection(rdd2).count
		intersectionCount == size1
	}

	def cleanedGroupedAliasesTestRDD(): RDD[WikipediaLinkAnalysis.Link] = {
		sc.parallelize(List(
			WikipediaLinkAnalysis.Link("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Bayern", Map("Bayern" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			WikipediaLinkAnalysis.Link("Zerfall", Map("Zerfall (Album)" -> 1).toSeq)
		))
	}

	def allAliasesTestRDD(): List[String] = {
		List(
			"Audi",
			"Brachttal",
			"Main-Kinzig-Kreis",
			"Hessen",
			"1377",
			"Büdinger Wald"
		)
	}

	def parsedWikipediaTestRDD(): RDD[WikipediaTextparser.ParsedWikipediaEntry] = {
		sc.parallelize(List(
			WikipediaTextparser.ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					WikipediaTextparser.Link("Audi", "Audi", 9)
				)),
			WikipediaTextparser.ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List()),
			WikipediaTextparser.ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				List(
					WikipediaTextparser.Link("Brachttal", "Brachttal", 55),
					WikipediaTextparser.Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", 66),
					WikipediaTextparser.Link("Hessen", "Hessen", 87),
					WikipediaTextparser.Link("1377", "1377", 225),
					WikipediaTextparser.Link("Büdinger Wald", "Büdinger Wald", 546)
				))))
	}

	def aliasOccurrencesInArticlesTestRDD(): RDD[WikipediaAliasCounter.AliasOccurrencesInArticle] = {
		sc.parallelize(List(
			WikipediaAliasCounter.AliasOccurrencesInArticle(List("Audi"), List()),
			WikipediaAliasCounter.AliasOccurrencesInArticle(List(), List("Audi")),
			WikipediaAliasCounter.AliasOccurrencesInArticle(List("Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"), List())
		))
	}

	def aliasCounterTestRDD(): RDD[WikipediaAliasCounter.AliasCounter] = {
		sc.parallelize(List(
			WikipediaAliasCounter.AliasCounter("lorem ipsum")
		))
	}
}
