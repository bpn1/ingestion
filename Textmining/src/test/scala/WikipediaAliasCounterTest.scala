import WikipediaTextparser.ParsedWikipediaEntry
import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

class WikipediaAliasCounterTest extends FlatSpec with SharedSparkContext {
	"Counted aliases" should "have the same size as all links" in {
		val allAliases = allAliasesTestRDD()
		val countedAliases = WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD(), allAliases, sc)
		assert(countedAliases.count() == allAliases.count)
	}

	"Counted aliases" should "have the same aliases as all links" in {
		val countedAliases = WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD(), allAliasesTestRDD(), sc)
			.map(_.alias)
		assert(areRDDsEqual(countedAliases.asInstanceOf[RDD[Any]], allAliasesTestRDD().asInstanceOf[RDD[Any]]))
	}

	"Counted aliases" should "have counted any occurrence" in {
		val countedAliases = WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD(), allAliasesTestRDD(), sc)
			.collect
			.foreach(aliasCounter => assert(aliasCounter.totalOccurrences > 0))
	}

	"Counted aliases" should "have consistent counts" in {
		val countedAliases = WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD(), allAliasesTestRDD(), sc)
			.collect
			.foreach { aliasCounter =>
				assert(aliasCounter.linkOccurrences <= aliasCounter.totalOccurrences)
				assert(aliasCounter.linkOccurrences >= 0 && aliasCounter.totalOccurrences >= 0)
			}
	}

	"Alias occurrences" should "be correct identified as link or no link" in {
		val allAliasesList = allAliasesTestRDD().collect.toList
		val aliasOccurrencesInArticles = parsedWikipediaTestRDD()
			.map(article => WikipediaAliasCounter.identifyAliasOccurrencesInArticle(article, allAliasesList))
		assert(areRDDsEqual(aliasOccurrencesInArticles.asInstanceOf[RDD[Any]], aliasOccurrencesInArticlesTestRDD().asInstanceOf[RDD[Any]]))
	}

	"Counted aliases" should "be exactly these counted aliases" in {
		val countedAliases = WikipediaAliasCounter.countAllAliasOccurrences(parsedWikipediaTestRDD(), allAliasesTestRDD(), sc)
		assert(areRDDsEqual(countedAliases.asInstanceOf[RDD[Any]], countedAliasesTestRDD().asInstanceOf[RDD[Any]]))
	}

	def printRDDs(is: RDD[Any], should: RDD[Any]): Unit = {
		println("\nRDD: ")
		is
			.collect
			.foreach(println)

		println("\n Should be: ")
		should
			.collect
			.foreach(println)
	}

	def areRDDsEqual(is: RDD[Any], should: RDD[Any]): Boolean = {
		//		printRDDs(is, should)
		val sizeIs = is.count
		val sizeShould = should.count
		if (sizeIs != sizeShould)
			return false
		val intersectionCount = is.intersection(should).count
		intersectionCount == sizeIs
	}

	def allAliasesTestRDD(): RDD[String] = {
		sc.parallelize(List(
			"Audi",
			"Brachttal",
			"Main-Kinzig-Kreis",
			"Hessen",
			"1377",
			"Büdinger Wald"
		))
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

	def countedAliasesTestRDD(): RDD[WikipediaAliasCounter.AliasCounter] = {
		sc.parallelize(List(
			WikipediaAliasCounter.AliasCounter("Audi", 1, 2),
			WikipediaAliasCounter.AliasCounter("Brachttal", 1, 1),
			WikipediaAliasCounter.AliasCounter("Main-Kinzig-Kreis", 1, 1),
			WikipediaAliasCounter.AliasCounter("Hessen", 1, 1),
			WikipediaAliasCounter.AliasCounter("1377", 1, 1),
			WikipediaAliasCounter.AliasCounter("Büdinger Wald", 1, 1)
		))
	}
}
