import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import WikiClasses._

class WikipediaContextExtractorTest extends FlatSpec with PrettyTester with SharedSparkContext {
	"Link contexts" should "contain all occurring page names of links and only once" in {
		val pageNames = WikipediaContextExtractor.extractAllContexts(parsedWikipediaTestRDD())
			.map(_.pagename)
			.sortBy(identity)
		assert(areRDDsEqual(pageNames, allPageNamesTestRDD()))
	}

	"Link contexts" should "not be empty" in {
		val contexts = WikipediaContextExtractor.extractAllContexts(parsedWikipediaTestRDD())
		assert(!contexts.isEmpty())
	}

	"Link contexts" should "contain any words" in {
		WikipediaContextExtractor.extractAllContexts(parsedWikipediaTestRDD())
			.collect
			.foreach(context => assert(context.words.nonEmpty))
	}

	"Link contexts of one article" should "contain all occurring page names of links" in {
		val articleTitle = "Testartikel"
		val article = getArticle(articleTitle)
		val pageNames = WikipediaContextExtractor.extractLinkContextsFromArticle(article, new CleanCoreNLPTokenizer)
			.map(_.pagename)
		assert(pageNames == allPageNamesOfTestArticleList())
	}

	"Link contexts of one article" should "contain at least these words" in {
		val articleTitle = "Streitberg (Brachttal)"
		val testWords = articleContextwordSets()(articleTitle)
		val article = getArticle(articleTitle)
		WikipediaContextExtractor.extractLinkContextsFromArticle(article, new CleanCoreNLPTokenizer)
			.foreach(context => assert(isSubset(testWords, context.words)))
	}

	"Document frequencies" should "not be empty" in {
		val documentFrequencies = WikipediaContextExtractor.countDocumentFrequencies(parsedWikipediaTestRDD())
		assert(!documentFrequencies.isEmpty)
	}

	"Document frequencies" should "be greater than zero" in {
		WikipediaContextExtractor.countDocumentFrequencies(parsedWikipediaTestRDD())
			.collect
			.foreach(df => assert(df.count > 0))
	}

	"Word set from one article" should "be exactly this word set" in {
		val articleTitle = "Testartikel"
		val testWords = articleContextwordSets()(articleTitle)
		val article = getArticle(articleTitle)
		val wordSet = WikipediaContextExtractor.textToWordSet(article.text.get, new CleanCoreNLPTokenizer)
		assert(areSetsEqual(wordSet, testWords))
	}

	"Document frequencies" should "contain these document frequencies" in {
		val documentFrequencies = WikipediaContextExtractor.countDocumentFrequencies(parsedWikipediaTestRDD())
			.collect
			.toSet
		val testDocumentFrequencies = documentFrequenciesTestRDD()
			.collect
			.toSet
		assert(isSubset(testDocumentFrequencies, documentFrequencies))
	}

	def getArticle(title: String): ParsedWikipediaEntry = {
		parsedWikipediaTestRDD()
			.filter(_.title == title)
		    .first
	}

	def parsedWikipediaTestRDD(): RDD[ParsedWikipediaEntry] = {
		sc.parallelize(List(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", 9)
				),
				List("Audi")),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List(),
				List("Audi")),
			ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				List(
					Link("Brachttal", "Brachttal", 55),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", 66),
					Link("Hessen", "Hessen", 87),
					Link("1377", "1377", 225),
					Link("Büdinger Wald", "Büdinger Wald", 546)
				),
				List("Streitberg", "Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald")),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				List(
					Link("Audi", "Audi", 7),
					Link("Brachttal", "Brachttal", 13),
					Link("historisches Jahr", "1377", 24)
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

	def allPageNamesOfTestArticleList(): Set[String] = {
		Set(
			"Audi",
			"Brachttal",
			"1377"
		)
	}

	def articleContextwordSets(): Map[String, Set[String]] = {
		Map("Testartikel" ->
			Set("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen"),

			"Streitberg (Brachttal)" ->
				Set("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde",
					"Im", "Jahre", "1500", "ist", "von", "Stridberg", "die",
					"Vom", "Mittelalter", "bis", "ins", "Jahrhundert", "hatte", "der", "Ort", "Waldrechte"
				))
	}

	def documentFrequenciesTestRDD(): RDD[DocumentFrequency] = {
		sc.parallelize(List(
			DocumentFrequency("Audi", 3),
			DocumentFrequency("Backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1)
		))
	}
}
