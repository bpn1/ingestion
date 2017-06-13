package de.hpi.ingestion.textmining.models

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

// scalastyle:off line.size.limit
object TestData {
	def featureEntries(): List[FeatureEntry] = {
		List(
			FeatureEntry("a", "b", 0.1, 0.2, 0.8, correct = true),
			FeatureEntry("c", "d", 0.4, 0.0, 0.7),
			FeatureEntry("e", "f", 0.7, 1.0, 0.1, correct = true),
			FeatureEntry("g", "h", 0.8, 0.2, 0.3),
			FeatureEntry("i", "j", 0.9, 0.4, 0.7))
	}

	def labeledPoints(): List[LabeledPoint] = {
		List(
			LabeledPoint(1.0, new DenseVector(Array(0.1, 0.2, 0.8))),
			LabeledPoint(0.0, new DenseVector(Array(0.4, 0.0, 0.7))),
			LabeledPoint(1.0, new DenseVector(Array(0.7, 1.0, 0.1))),
			LabeledPoint(0.0, new DenseVector(Array(0.8, 0.2, 0.3))),
			LabeledPoint(0.0, new DenseVector(Array(0.9, 0.4, 0.7))))
	}

	def parsedEntryWithDifferentLinkTypes(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Origineller Titel",
			Option("In diesem Text könnten ganz viele verschiedene Links stehen."),
			textlinks = List(Link("Apfel", "Apfel", Option(0)), Link("Baum", "Baum", Option(4))),
			templatelinks = List(Link("Charlie", "Charlie C.")),
			categorylinks = List(Link("Dora", "Dora")),
			listlinks = List(Link("Fund", "Fund"), Link("Grieß", "Brei")),
			disambiguationlinks = List(Link("Esel", "Esel")))
	}

	def parsedEntryWithDifferentAndReducedLinkTypes(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Origineller Titel",
			Option("In diesem Text könnten ganz viele verschiedene Links stehen."),
			textlinks = List(Link("Apfel", "Apfel", Option(0)), Link("Baum", "Baum", Option(4))),
			templatelinks = List(Link("Charlie", "Charlie C.")),
			categorylinks = List(Link("Dora", "Dora")),
			listlinks = List(Link("Fund", "Fund"), Link("Grieß", "Brei")),
			disambiguationlinks = List(Link("Esel", "Esel")),
			textlinksreduced = List(Link("Baum", "Baum", Option(4))),
			templatelinksreduced = Nil,
			categorylinksreduced = Nil,
			listlinksreduced = List(Link("Fund", "Fund")),
			disambiguationlinksreduced = List(Link("Esel", "Esel")))
	}

	def textLinksWithContext(): List[Link] = {
		List(
			Link("Hochhaus", "Hochhaus", Option(113), Map("test 1" -> 1)),
			Link("Berliner", "Berlin", Option(190), Map("test 3" -> 3)))
	}

	def extendedLinksWithContext(): List[Link] = {
		List(
			Link("Postbank", "Postbank", Option(126), Map("test 2" -> 2)),
			Link("Kreuzberg", "Berlin-Kreuzberg", Option(208), Map("test 4" -> 4)))
	}

	def parsedEntryWithFilteredLinks(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Origineller Titel",
			Option("In diesem Text könnten ganz viele verschiedene Links stehen."),
			textlinks = List(Link("Apfel", "Apfel", Option(0)), Link("Baum", "Baum", Option(4))),
			categorylinks = List(Link("Dora", "Dora")),
			listlinks = List(Link("Fund", "Fund")),
			disambiguationlinks = List(Link("Esel", "Esel"))
		)
	}

	def allLinksListFromEntryList(): List[Link] = {
		List(
			Link("Apfel", "Apfel", Option(0)),
			Link("Baum", "Baum", Option(4)),
			Link("Charlie", "Charlie C."),
			Link("Dora", "Dora"),
			Link("Fund", "Fund"),
			Link("Grieß", "Brei"),
			Link("Esel", "Esel"))
	}

	def allReducedLinks(): List[Link] = {
		List(
			Link("Baum", "Baum", Option(4)),
			Link("Fund", "Fund"),
			Link("Esel", "Esel"))
	}

	def parsedEntryWithContext(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Test Article",
			textlinks = List(
				Link("Hochhaus", "Hochhaus", Option(113)),
				Link("Berliner", "Berlin", Option(190))),
			rawextendedlinks = List(
				ExtendedLink("Postbank", Map("Postbank" -> 10), Option(126)),
				ExtendedLink("Kreuzberg", Map("Berlin-Kreuzberg" -> 10), Option(208))),
			linkswithcontext = List(
				Link("Hochhaus", "Hochhaus", Option(113), Map("test 1" -> 1)),
				Link("Postbank", "Postbank", Option(126), Map("test 2" -> 2)),
				Link("Berliner", "Berlin", Option(190), Map("test 3" -> 3)),
				Link("Kreuzberg", "Berlin-Kreuzberg", Option(208), Map("test 4" -> 4)))
		)
	}

	def parsedEntryWithReducedLinks(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Origineller Titel",
			Option("In diesem Text könnten ganz viele verschiedene Links stehen."),
			textlinks = List(Link("Apfel", "Apfel", Option(0)), Link("Baum", "Baum", Option(4))),
			templatelinks = List(Link("Charlie", "Charlie C.")),
			categorylinks = List(Link("Dora", "Dora")),
			listlinks = List(Link("Fund", "Fund"), Link("Grieß", "Brei")),
			disambiguationlinks = List(Link("Esel", "Esel")),
			textlinksreduced = List(Link("Apfel", "Apfel", Option(0)), Link("Baum", "Baum", Option(4))),
			categorylinksreduced = List(Link("Dora", "Dora")),
			listlinksreduced = List(Link("Fund", "Fund")),
			disambiguationlinksreduced = List(Link("Esel", "Esel"))
		)
	}

	def trieAliases(): List[TrieAlias] = {
		List(
			TrieAlias("alias 1", Option(1), Map("a" -> 1, "b" -> 2)),
			TrieAlias("alias 2", Option(7), Map("c" -> 3, "d" -> 4)),
			TrieAlias("alias 3", Option(4)),
			TrieAlias("alias 4", None, Map("e" -> 5)),
			TrieAlias("alias 5", None))
	}

	def trieAliasLinks(): List[Link] = {
		List(
			Link("alias 1", null, Option(1), Map("a" -> 1, "b" -> 2)),
			Link("alias 2", null, Option(7), Map("c" -> 3, "d" -> 4)),
			Link("alias 3", null, Option(4)),
			Link("alias 4", null, None, Map("e" -> 5)),
			Link("alias 5", null, None))
	}

	def edgeCaseExtendedLinks(): List[ExtendedLink] = {
		List(
			ExtendedLink("Berlin", Map("Berlin" -> 5), Option(0)),
			ExtendedLink("Berlin", Map("Berlin" -> 20, "Brandenburg" -> 1), Option(1)),
			ExtendedLink("Berlin", Map("Berlin" -> 20, "Brandenburg" -> 1, "Backfisch" -> 1), Option(2)),
			ExtendedLink("Berlin", Map("Berlin" -> 5, "Brandenburg" -> 1), Option(3)),
			ExtendedLink("Berlin", Map("Berlin" -> 5, "Brandenburg" -> 1, "Backfisch" -> 1), Option(4)),
			ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 2, "Backfisch" -> 1), Option(5)),
			ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 99, "Backfisch" -> 13), Option(6)),
			ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 99, "Backfisch" -> 1), Option(7))
		)
	}

	def edgeCaseFilteredExtendedLinkPages(): List[Option[String]] = {
		List(
			Option("Berlin"),
			Option("Berlin"),
			Option("Berlin"),
			None,
			None,
			None,
			None,
			None
		)
	}

	def extendedLinksParsedWikipediaEntry(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Berlin",
			Option("Berlin ist cool"),
			textlinks = List (
				Link("Berlin", "Berlin", Option(0)),
				Link("Berlin", "Berlin", Option(15))
			),
			rawextendedlinks = List(
				ExtendedLink("Berlin", Map("Berlin" -> 5), Option(0)),
				ExtendedLink("Berlin", Map("Berlin" -> 20, "Brandenburg" -> 1), Option(1)),
				ExtendedLink("Berlin", Map("Berlin" -> 20, "Brandenburg" -> 1, "Backfisch" -> 1), Option(2)),
				ExtendedLink("Berlin", Map("Berlin" -> 5, "Brandenburg" -> 1), Option(3)),
				ExtendedLink("Berlin", Map("Berlin" -> 5, "Brandenburg" -> 1, "Backfisch" -> 1), Option(4)),
				ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 2, "Backfisch" -> 1), Option(5)),
				ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 99, "Backfisch" -> 13), Option(6)),
				ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 99, "Backfisch" -> 1), Option(7))
			)
		)
	}

	def entryWithFeatureEntryLinks(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Berlin",
			Option("Berlin ist cool"),
			textlinks = List (
				Link("Berlin", "Berlin", Option(0)),
				Link("Berlin", "Berlin", Option(15))
			),
			rawextendedlinks = List(
				ExtendedLink("Berlin", Map("Berlin" -> 5), Option(0)),
				ExtendedLink("Berlin", Map("Berlin" -> 20, "Brandenburg" -> 1), Option(1)),
				ExtendedLink("Berlin", Map("Berlin" -> 20, "Brandenburg" -> 1, "Backfisch" -> 1), Option(2)),
				ExtendedLink("Berlin", Map("Berlin" -> 5, "Brandenburg" -> 1), Option(3)),
				ExtendedLink("Berlin", Map("Berlin" -> 5, "Brandenburg" -> 1, "Backfisch" -> 1), Option(4)),
				ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 2, "Backfisch" -> 1), Option(5)),
				ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 99, "Backfisch" -> 13), Option(6)),
				ExtendedLink("Berlin", Map("Berlin" -> 100, "Brandenburg" -> 99, "Backfisch" -> 1), Option(7))
			),
			linkswithcontext = List(
				Link("Berlin", "Berlin", Option(0)),
				Link("Berlin", "Berlin", Option(15)),
				Link("Berlin", "Berlin", Option(1)),
				Link("Berlin", "Berlin", Option(2))
			),
			triealiases = List(
				TrieAlias("alias 1", Option(1)),
				TrieAlias("alias 2", Option(2)),
				TrieAlias("alias 3", Option(3))
			)
		)
	}

	def parsedWikipediaEntryWithLinkCollisions(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Berlin",
			Option("Berlin ist cool"),
			textlinks = List(
				Link("Berlin", "Berlin", Option(0)),
				Link("Berlin", "Berlin", Option(10)),
				Link("Berlin", "Berlin", Option(20)),
				Link("Berlin", "Berlin", Option(30)),
				Link("Berlin", "Berlin", Option(40)),
				Link("Berlin", "Backfisch", Option(50)),
				Link("Berlin", "Berlin", Option(60)),
				Link("Berlin", "Berlin", Option(90)),
				Link("Berlin", "Berlin", Option(100)),
				Link("Berlin", "Berlin", Option(120)),
				Link("Berlin", "Berlin", Option(130)),
				Link("Berlin", "Berlin", Option(140)),
				Link("Berlin", "Berlin", Option(160)),
				Link("Berlin", "Berlin", Option(160))
			),
			rawextendedlinks = List(
				ExtendedLink("BER", Map("Berlin" -> 5), Option(0)),
				ExtendedLink("BER", Map("Berlin" -> 5), Option(11)),
				ExtendedLink("Berliner", Map("Berlin" -> 5), Option(18)),
				ExtendedLink("Berlin", Map("Berlin" -> 5), Option(30)),
				ExtendedLink("Berliner", Map("Berlin" -> 5), Option(41)),
				ExtendedLink("Backfische", Map("Backfisch" -> 5), Option(49)),
				ExtendedLink("Berliner", Map("Berlin" -> 5), Option(60)),
				ExtendedLink("Berlin", Map("Berlin" -> 5), Option(70)),
				ExtendedLink("Berlin", Map("Berlin" -> 5), Option(80)),
				ExtendedLink("B", Map("Berlin" -> 5), Option(90)),
				ExtendedLink("E", Map("Berlin" -> 5), Option(91)),
				ExtendedLink("R", Map("Berlin" -> 5), Option(92)),
				ExtendedLink("L", Map("Berlin" -> 5), Option(93)),
				ExtendedLink("I", Map("Berlin" -> 5), Option(94)),
				ExtendedLink("N", Map("Berlin" -> 5), Option(95)),
				ExtendedLink("BER", Map("Berlin" -> 5), Option(98)),
				ExtendedLink("BER", Map("Berlin" -> 5), Option(125)),
				ExtendedLink("BER", Map("Berlin" -> 5), Option(139)),
				ExtendedLink("BER", Map("Berlin" -> 5), Option(163)),
				ExtendedLink("BER", Map("Berlin" -> 5), Option(180)),
				ExtendedLink("BER", Map("Berlin" -> 5), Option(200))
			)
		)
	}

	def featureEntryLinks(): List[Link] = {
		List(
			Link("Berlin", "Berlin", Option(0)),
			Link("Berlin", "Berlin", Option(15)),
			Link("Berlin", "Berlin", Option(1)),
			Link("Berlin", "Berlin", Option(2)),
			Link("alias 1", null, Option(1)),
			Link("alias 2", null, Option(2)),
			Link("alias 3", null, Option(3))
		)
	}

	def linksWithoutCollisions(): List[Link] = {
		List(
			Link("Berlin", "Berlin", Option(0)),
			Link("Berlin", "Berlin", Option(10)),
			Link("Berlin", "Berlin", Option(20)),
			Link("Berlin", "Berlin", Option(30)),
			Link("Berlin", "Berlin", Option(40)),
			Link("Berlin", "Backfisch", Option(50)),
			Link("Berlin", "Berlin", Option(60)),
			Link("Berlin", "Berlin", Option(90)),
			Link("Berlin", "Berlin", Option(100)),
			Link("Berlin", "Berlin", Option(120)),
			Link("Berlin", "Berlin", Option(130)),
			Link("Berlin", "Berlin", Option(140)),
			Link("Berlin", "Berlin", Option(160)),
			Link("Berlin", "Berlin", Option(160)),
			Link("Berlin", "Berlin", Option(70)),
			Link("Berlin", "Berlin", Option(80)),
			Link("BER", "Berlin", Option(180)),
			Link("BER", "Berlin", Option(200))
		)
	}

	def edgeCaseExtendedLinksToLinks(): List[Link] = {
		List(
			Link("Berlin", "Berlin", Option(0)),
			Link("Berlin", "Berlin", Option(1)),
			Link("Berlin", "Berlin", Option(2))
		)
	}

	def idfMap(): Map[String, Double] = {
		Map(
			"term 1" -> 0.123,
			"term 2" -> 0.345,
			"term 3" -> 0.876
		)
	}

	def inverseDocumentFrequenciesSet(): Set[(String, Double)] = {
		val oneOccurrence = 0.6020599913279624
		val threeOccurrences = 0.12493873660829993
		Set(
			("audi", threeOccurrences),
			("backfisch", oneOccurrence))
	}

	def tfidfContextsSet(): Set[(String, Map[String, Double])] = {
		val tf1df1 = 0.47712125471966244
		val tf1df2 = 0.17609125905568124
		val tf2df1 = 0.9542425094393249
		val df3 = 0.0
		Set(
			("Audi Test mit Link", Map("audi" -> df3, "verlink" -> tf1df2)),
			("Audi Test ohne Link", Map("audi" -> df3, "verlink" -> tf1df2)),
			("Testartikel", Map("audi" -> df3, "brachttal" -> tf1df1, "historisch" -> tf1df1, "jahr" -> tf1df1, "hess" -> tf2df1, "main-kinzig-kreis" -> tf1df1, "buding" -> tf1df1, "wald" -> tf1df1, "backfisch" -> tf1df1, "nochmal" -> tf1df1))
		)
	}

	def articlesWithLinkAndAliasContexts(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				textlinks = List(Link("Audi", "Audi", Option(9))),
				linkswithcontext = List(Link("Audi", "Audi", Option(9), Map("verlink" -> 1)))
			),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt.")),
			ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				textlinks = List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Hessen", "Hessen", Option(87)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546))),
				linkswithcontext = List(
					Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
					Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1))),
				triealiases = List(
					TrieAlias("Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
					TrieAlias("1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)))
			),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				textlinks = List(
					Link("Audi", "Audi", Option(7)),
					Link("historisches Jahr", "1377", Option(24))),
				linkswithcontext = List(
					Link("Audi", "Audi", Option(7), Map("brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
					Link("historisches Jahr", "1377", Option(24), Map("audi" -> 1, "brachttal" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1))),
				triealiases = List(
					TrieAlias("Brachttal", Option(13), Map("audi" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
			))
	}

	def linkContextsTfidfWithTrieAliases(): Set[(Link, Map[String, Double])] = {
		// retrieved from 4 documents
		val tf1df1 = 0.6020599913279624
		val tf1df2 = 0.3010299956639812
		val tf1df3 = 0.12493873660829993
		val tf2df1 = 1.2041199826559248
		val tf2df2 = 0.6020599913279624
		val tf3df2 = 0.9030899869919435
		Set(
			(Link("Audi", "Audi", Option(9)), Map("verlink" -> tf1df2)),
			(Link("Brachttal", "Brachttal", Option(55)), Map("einwohnerzahl" -> tf1df1, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "hess" -> tf1df2, "gemei" -> tf1df1, "main-kinzig-kreis" -> tf1df2, "streitberg" -> tf1df1)),
			(Link("Main-Kinzig-Kreis", null, Option(66)), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "hess" -> tf1df2, "gemei" -> tf1df1, "streitberg" -> tf1df1)),
			(Link("Hessen", "Hessen", Option(87)), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "gemei" -> tf1df1, "main-kinzig-kreis" -> tf1df2, "streitberg" -> tf1df1)),
			(Link("1377", null, Option(225)), Map("einwohnerzahl" -> tf1df1, "streidtburgk" -> tf1df1, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf1df1, "bezeichnung" -> tf1df1, "jahr" -> tf3df2, "270" -> tf1df1, "stridberg" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "tauch" -> tf1df1, "1500" -> tf1df1, "namensvaria" -> tf1df1, "red" -> tf1df1)),
			(Link("Büdinger Wald", "Büdinger Wald", Option(546)), Map("waldrech" -> tf1df1, "19" -> tf1df1, "ort" -> tf1df1, "jahrhu" -> tf1df1, "-lrb-" -> tf1df1, "huterech" -> tf1df1, "eingeburg" -> tf1df1, "-" -> tf1df1, "-rrb-" -> tf1df1, "holx" -> tf1df1, "ortsnam" -> tf1df1, "streitberg" -> tf1df1, "mittelal" -> tf1df1)),
			(Link("Audi", "Audi", Option(7)), Map("brachttal" -> tf1df2, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("Brachttal", null, Option(13)), Map("audi" -> tf1df3, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("historisches Jahr", "1377", Option(24)), Map("audi" -> tf1df3, "brachttal" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)))
	}
}
// scalastyle:on line.size.limit
