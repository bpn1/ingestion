import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD

class WikipediaContextExtractorTest extends FlatSpec with SharedSparkContext {
	"Link contexts" should "contain all occurring page names of links and only once" in {
		val pageNames = WikipediaContextExtractor.extractAllContexts(parsedWikipediaTestRDD())
			.map(_.pagename)
			.sortBy(identity)
		assert(areRDDsEqual(pageNames.asInstanceOf[RDD[Any]], allPageNamesTestRDD().asInstanceOf[RDD[Any]]))
	}

	"Link contexts" should "not be empty" in {
		val contexts = WikipediaContextExtractor.extractAllContexts(parsedWikipediaTestRDD())
		assert(!contexts.isEmpty())
		contexts
			.collect
			.foreach(context => assert(context.words.nonEmpty))
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
					WikiClasses.Link("Hessen", "1377", 225),
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

	def allPageNamesTestRDD(): RDD[String] = {
		sc.parallelize(List(
			"Brachttal",
			"Main-Kinzig-Kreis",
			"Hessen",
			"1377",
			"Büdinger Wald",
			"Audi"
		))
			.sortBy(identity)
	}
}
