import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

class WikipediaAliasCounterTest extends FlatSpec with SharedSparkContext {
	"Counted aliases" should "have the same size as all links" in {
		val allAliases = allAliasesTestRDD()
		val countedAliases = WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD())
		assert(countedAliases.count() == allAliases.count)
	}

	"Counted aliases" should "have the same aliases as all links" in {
		val countedAliases = WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD())
			.map(_.alias)
			.sortBy(identity)
		assert(areRDDsEqual(countedAliases.asInstanceOf[RDD[Any]], allAliasesTestRDD().asInstanceOf[RDD[Any]]))
	}

	"Counted aliases" should "have counted any occurrence" in {
		WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD())
			.collect
			.foreach(aliasCounter => assert(aliasCounter.totalOccurrences > 0))
	}

	"Counted aliases" should "have consistent counts" in {
		WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD())
			.collect
			.foreach { aliasCounter =>
				assert(aliasCounter.linkOccurrences <= aliasCounter.totalOccurrences)
				assert(aliasCounter.linkOccurrences >= 0 && aliasCounter.totalOccurrences >= 0)
			}
	}

	"Counted aliases" should "be exactly these counted aliases" in {
		val countedAliases = WikipediaAliasCounter
			.countAllAliasOccurrences(parsedWikipediaTestRDD())
			.sortBy(_.alias)
		assert(areRDDsEqual(countedAliases.asInstanceOf[RDD[Any]], countedAliasesTestRDD().asInstanceOf[RDD[Any]]))
	}

	"Alias occurrences" should "be correct identified as link or no link" in {
		val aliasOccurrencesInArticles = parsedWikipediaTestRDD()
			.map(article => WikipediaAliasCounter.identifyAliasOccurrencesInArticle(article))
		assert(areRDDsEqual(aliasOccurrencesInArticles.asInstanceOf[RDD[Any]], aliasOccurrencesInArticlesTestRDD().asInstanceOf[RDD[Any]]))
	}

	"Identified aliases" should "not be link and no link in the same article" in {
		parsedWikipediaTestRDD()
			.map(article => WikipediaAliasCounter.identifyAliasOccurrencesInArticle(article))
			.collect
			.foreach(occurrences => assert(occurrences.links.intersect(occurrences.noLinks).isEmpty))
	}

	"Probability that word is link" should "be calculated correctly" in {
		val linkProbabilities = countedAliasesTestRDD()
			.map(countedAlias => (countedAlias.alias, WikipediaAliasCounter.probabilityIsLink(countedAlias)))
		assert(areRDDsEqual(linkProbabilities.asInstanceOf[RDD[Any]], linkProbabilitiesTestRDD().asInstanceOf[RDD[Any]]))
	}

	def printRDD(rdd: RDD[Any], title: String = ""): Unit = {
		println(title)
		rdd
			.collect
			.foreach(println)
	}

	def areRDDsEqual(is: RDD[Any], should: RDD[Any]): Boolean = {
		//		printRDD(is, "\nRDD:")
		//		printRDD(should, "\nShould be:")
		val sizeIs = is.count
		val sizeShould = should.count
		if (sizeIs != sizeShould)
			return false
		val diff = is
			.collect
			.zip(should.collect)
			.collect { case (a, b) if a != b => a -> b }
		diff.isEmpty
	}

	def allAliasesTestRDD(): RDD[String] = {
		sc.parallelize(List(
			"Audi",
			"Brachttal",
			"Main-Kinzig-Kreis",
			"Hessen",
			"1377",
			"Büdinger Wald",
			"Backfisch",
			"Streitberg",
			"historisches Jahr"
		))
			.sortBy(identity)
	}

	def parsedWikipediaTestRDD(): RDD[WikiClasses.ParsedWikipediaEntry] = {
		sc.parallelize(List(
			WikiClasses.ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					WikiClasses.Link("Audi", "Audi", 9)
				),
				List("Audi")),
			WikiClasses.ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List(),
				List("Audi")),
			WikiClasses.ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				List(
					WikiClasses.Link("Brachttal", "Brachttal", 55),
					WikiClasses.Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", 66),
					WikiClasses.Link("Hessen", "Hessen", 87),
					WikiClasses.Link("1377", "1377", 225),
					WikiClasses.Link("Büdinger Wald", "Büdinger Wald", 546)
				),
				List("Streitberg", "Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald")),
			WikiClasses.ParsedWikipediaEntry("Testartikel", Option("""Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."""),
				List(
					WikiClasses.Link("Audi", "Audi", 7),
					WikiClasses.Link("Brachttal", "Brachttal", 13),
					WikiClasses.Link("historisches Jahr", "1377", 24)
				),
				List("Audi", "Brachttal", "historisches Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"))))
	}

	def aliasOccurrencesInArticlesTestRDD(): RDD[WikiClasses.AliasOccurrencesInArticle] = {
		sc.parallelize(List(
			WikiClasses.AliasOccurrencesInArticle(Set("Audi"), Set()),
			WikiClasses.AliasOccurrencesInArticle(Set(), Set("Audi")),
			WikiClasses.AliasOccurrencesInArticle(Set("Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"), Set("Streitberg")),
			WikiClasses.AliasOccurrencesInArticle(Set("Audi", "Brachttal", "historisches Jahr"), Set("Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"))
		))
	}

	def countedAliasesTestRDD(): RDD[WikiClasses.AliasCounter] = {
		sc.parallelize(List(
			WikiClasses.AliasCounter("Audi", 2, 3),
			WikiClasses.AliasCounter("Brachttal", 2, 2),
			WikiClasses.AliasCounter("Main-Kinzig-Kreis", 1, 2),
			WikiClasses.AliasCounter("Hessen", 1, 2),
			WikiClasses.AliasCounter("1377", 1, 1),
			WikiClasses.AliasCounter("Büdinger Wald", 1, 2),
			WikiClasses.AliasCounter("Backfisch", 0, 1),
			WikiClasses.AliasCounter("Streitberg", 0, 1),
			WikiClasses.AliasCounter("historisches Jahr", 1, 1)
		))
			.sortBy(_.alias)
	}

	def linkProbabilitiesTestRDD(): RDD[Tuple2[String, Double]] = {
		sc.parallelize(List(
			("Audi", 2.0 / 3),
			("Brachttal", 1.0),
			("Main-Kinzig-Kreis", 1.0 / 2),
			("Hessen", 1.0 / 2),
			("1377", 1.0),
			("Büdinger Wald", 1.0 / 2),
			("Backfisch", 0.0),
			("Streitberg", 0.0),
			("historisches Jahr", 1.0)
		))
			.sortBy(_._1)
	}
}
