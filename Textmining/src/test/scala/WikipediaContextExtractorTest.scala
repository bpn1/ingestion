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
	}

	"Link contexts" should "contain any words" in {
		WikipediaContextExtractor.extractAllContexts(parsedWikipediaTestRDD())
			.collect
			.foreach(context => assert(context.words.nonEmpty))
	}

	"Link contexts of one article" should "contain all occurring page names of links" in {
		val articleTitle = "Testartikel"
		val article = getArticle(articleTitle)
		val pageNames = WikipediaContextExtractor.extractLinkContextsFromArticle(article, new WhitespaceTokenizer)
			.map(_.pagename)
		assert(pageNames == allPageNamesOfTestArticleList())
	}

	"Link contexts of one article" should "contain at least these words" in {
		val articleTitle = "Streitberg (Brachttal)"
		val testWords = articleContextwordSets()(articleTitle)
		val article = getArticle(articleTitle)
		WikipediaContextExtractor.extractLinkContextsFromArticle(article, new WhitespaceTokenizer)
			.foreach(context => assert(isSubset(testWords.asInstanceOf[Set[Any]], context.words.asInstanceOf[Set[Any]])))
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
		val testWords: Set[String] = articleContextwordSets()(articleTitle)
		val article = getArticle(articleTitle)
		val wordSet: Set[String] = WikipediaContextExtractor.textToWordSet(article.text.get, new WhitespaceTokenizer)
		assert(areSetsEqual(wordSet.asInstanceOf[Set[Any]], testWords.asInstanceOf[Set[Any]]))
	}

	"Document frequencies" should "contain these document frequencies" in {
		val documentFrequencies = WikipediaContextExtractor.countDocumentFrequencies(parsedWikipediaTestRDD())
			.collect
			.toSet
		val testDocumentFrequencies = documentFrequenciesTestRDD()
			.collect
			.toSet
		assert(isSubset(testDocumentFrequencies.asInstanceOf[Set[Any]], documentFrequencies.asInstanceOf[Set[Any]]))
	}

	def printSequence(sequence: Seq[Any], title: String = ""): Unit = {
		println(title)
		sequence
			.foreach(println)
	}

	def areRDDsEqual(is: RDD[Any], should: RDD[Any]): Boolean = {
		//		printSequence(is.collect, "\nRDD:")
		//		printSequence(should.collect, "\nShould be:")
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

	def isSubset(subset: Set[Any], set: Set[Any]): Boolean = {
		//		printSequence(subset.toSeq, "\nSet:")
		//		printSequence(set.toSeq, "\nShould be subset of:")
		subset.subsetOf(set)
	}

	def areSetsEqual(is: Set[Any], should: Set[Any]): Boolean = {
		//		printSequence(is.toSeq, "\nSet:")
		//		printSequence(should.toSeq, "\nShould be:")
		is == should
	}

	def getArticle(title: String): WikiClasses.ParsedWikipediaEntry = {
		parsedWikipediaTestRDD()
			.filter(_.title == title)
			.collect
			.head
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
			Set("Links:", "Audi,", "Brachttal,", "historisches", "Jahr.\\nKeine", "Hessen,", "Main-Kinzig-Kreis,", "Büdinger", "Wald,", "Backfisch", "und", "nochmal", "Hessen."),

			"Streitberg (Brachttal)" ->
				Set("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde",
					"Im", "Jahre", "1500", "ist", "von", "Stridberg", "die",
					"Vom", "Mittelalter", "bis", "ins", "19.", "Jahrhundert", "hatte", "der", "Ort", "Waldrechte"
				))
	}

	def documentFrequenciesTestRDD(): RDD[WikiClasses.DocumentFrequency] = {
		sc.parallelize(List(
			WikiClasses.DocumentFrequency("Audi", 2),
			WikiClasses.DocumentFrequency("Backfisch", 1),
			WikiClasses.DocumentFrequency("ist", 3),
			WikiClasses.DocumentFrequency("und", 2),
			WikiClasses.DocumentFrequency("zugleich", 1)
		))
	}
}
