package de.hpi.ingestion.textmining

import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._

import scala.io.Source

// scalastyle:off number.of.methods
object TestData {
	// scalastyle:off line.size.limit

	def testSentences(): List[String] = {
		List(
			"This is a test sentence.",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen.",
			"Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."
		)
	}

	def tokenizedTestSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "in", "Hessen"),
			List("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine", "Links", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen")
		)
	}

	def allAliasesTestRDD(sc: SparkContext): RDD[String] = {
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

	def parsedWikipediaTestSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", 9)
				),
				List(),
				List("Audi")),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List(),
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
				List(),
				List("Streitberg", "Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald")),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				List(
					Link("Audi", "Audi", 7),
					Link("Brachttal", "Brachttal", 13),
					Link("historisches Jahr", "1377", 24)
				),
				List(),
				List("Audi", "Brachttal", "historisches Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch")))
	}

	def aliasOccurrencesInArticlesTestRDD(sc: SparkContext): RDD[AliasOccurrencesInArticle] = {
		sc.parallelize(List(
			AliasOccurrencesInArticle(Set("Audi"), Set()),
			AliasOccurrencesInArticle(Set(), Set("Audi")),
			AliasOccurrencesInArticle(Set("Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"), Set("Streitberg")),
			AliasOccurrencesInArticle(Set("Audi", "Brachttal", "historisches Jahr"), Set("Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"))
		))
	}

	def startAliasCounterTestRDD(sc: SparkContext): RDD[AliasCounter] = {
		sc.parallelize(List(
			AliasCounter("Audi", 1, 1),
			AliasCounter("Audi", 1, 1),
			AliasCounter("Audi", 0, 1),
			AliasCounter("Brachttal", 1, 1),
			AliasCounter("Brachttal", 1, 1),
			AliasCounter("Main-Kinzig-Kreis", 1, 1),
			AliasCounter("Main-Kinzig-Kreis", 0, 1),
			AliasCounter("Hessen", 1, 1),
			AliasCounter("Hessen", 0, 1),
			AliasCounter("1377", 1, 1),
			AliasCounter("Büdinger Wald", 1, 1),
			AliasCounter("Büdinger Wald", 0, 1),
			AliasCounter("Backfisch", 0, 1),
			AliasCounter("Streitberg", 0, 1),
			AliasCounter("historisches Jahr", 1, 1)
		))
	}

	def countedAliasesTestRDD(sc: SparkContext): RDD[AliasCounter] = {
		sc.parallelize(List(
			AliasCounter("Audi", 2, 3),
			AliasCounter("Brachttal", 2, 2),
			AliasCounter("Main-Kinzig-Kreis", 1, 2),
			AliasCounter("Hessen", 1, 2),
			AliasCounter("1377", 1, 1),
			AliasCounter("Büdinger Wald", 1, 2),
			AliasCounter("Backfisch", 0, 1),
			AliasCounter("Streitberg", 0, 1),
			AliasCounter("historisches Jahr", 1, 1)
		))
	}

	def linkProbabilitiesTestRDD(sc: SparkContext): RDD[(String, Double)] = {
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

	def getArticle(title: String): ParsedWikipediaEntry = {
		parsedWikipediaTestSet()
			.filter(_.title == title)
			.head
	}

	def allPageNamesTestRDD(sc: SparkContext): RDD[String] = {
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
		Map(
			"Testartikel" -> Set("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine",
				"Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen"),
			"Streitberg (Brachttal)" ->
				Set("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde",
					"Im", "Jahre", "1500", "ist", "von", "Stridberg", "die", "Vom", "Mittelalter",
					"bis", "ins", "Jahrhundert", "hatte", "der", "Ort", "Waldrechte"))
	}

	def documentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("Audi", 3),
			DocumentFrequency("Backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1),
			DocumentFrequency("einer", 1)
		)
	}

	def filteredDocumentFrequenciesTestList(): List[DocumentFrequency] = {
		List(
			DocumentFrequency("Audi", 3)
		)
	}

	def requestedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("Audi", 3),
			DocumentFrequency("Backfisch", 2),
			DocumentFrequency("ist", 2),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 2),
			DocumentFrequency("einer", 2)
		)
	}

	def unstemmedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("Audi", 3),
			DocumentFrequency("Backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1),
			DocumentFrequency("einer", 1),
			DocumentFrequency("ein", 2),
			DocumentFrequency("Einer", 1),
			DocumentFrequency("Ein", 1)
		)
	}

	def stemmedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1),
			DocumentFrequency("ein", 5)
		)
	}

	def unstemmedGermanWordsTestSet(): Set[String] = {
		Set("Es", "Keine", "Kein", "Keiner", "Ist", "keine", "keiner", "brauchen", "können", "sollte")
	}

	// scalastyle:off method.length
	def wikipediaTestReferences(): Map[String, List[Link]] = {
		// extracted links from Article abstracts
		Map(
			"Audi" -> List(
				// Article text links
				Link("Ingolstadt", "Ingolstadt", 55),
				Link("Bayern", "Bayern", 69),
				Link("Automobilhersteller", "Automobilhersteller", 94),
				Link("Volkswagen", "Volkswagen AG", 123),
				Link("Wortspiel", "Wortspiel", 175),
				Link("Namensrechte", "Marke (Recht)", 202),
				Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", 255),
				Link("August Horch", "August Horch", 316),
				Link("Lateinische", "Latein", 599),
				Link("Imperativ", "Imperativ (Modus)", 636),
				Link("Zwickau", "Zwickau", 829),
				Link("Zschopauer", "Zschopau", 868),
				Link("DKW", "DKW", 937),
				Link("Wanderer", "Wanderer (Unternehmen)", 1071),
				Link("Auto Union AG", "Auto Union", 1105),
				Link("Chemnitz", "Chemnitz", 1131),
				Link("Zweiten Weltkrieg", "Zweiter Weltkrieg", 1358),
				Link("Ingolstadt", "Ingolstadt", 1423),
				Link("NSU Motorenwerke AG", "NSU Motorenwerke", 1599),
				Link("Neckarsulm", "Neckarsulm", 1830),

				// Template links
				Link("Aktiengesellschaft", "Aktiengesellschaft"),
				Link("Zwickau", "Zwickau"),
				Link("Chemnitz", "Chemnitz"),
				Link("Ingolstadt", "Ingolstadt"),
				Link("Neckarsulm", "Neckarsulm"),
				Link("Ingolstadt", "Ingolstadt"),
				Link("Deutschland", "Deutschland"),
				Link("Rupert Stadler", "Rupert Stadler"),
				Link("Vorstand", "Vorstand"),
				Link("Matthias Müller", "Matthias Müller (Manager)"),
				Link("Aufsichtsrat", "Aufsichtsrat"),
				Link("Mrd.", "Milliarde"),
				Link("EUR", "Euro"),
				Link("Automobilhersteller", "Automobilhersteller")),

			"Electronic Arts" -> List(
				// Article text links
				Link("Publisher", "Publisher", 83),
				Link("Computer- und Videospielen", "Computerspiel", 97),
				Link("Madden NFL", "Madden NFL", 180),
				Link("FIFA", "FIFA (Spieleserie)", 192),
				Link("Vivendi Games", "Vivendi Games", 346),
				Link("Activision", "Activision", 364),
				Link("Activision Blizzard", "Activision Blizzard", 378),
				Link("Nasdaq Composite", "Nasdaq Composite", 675),
				Link("S&P 500", "S&P 500", 699),

				// Template links
				Link("Corporation", "Gesellschaftsrecht der Vereinigten Staaten#Corporation"),
				Link("Redwood City", "Redwood City"),
				Link("USA", "Vereinigte Staaten"),
				Link("Larry Probst", "Larry Probst"),
				Link("USD", "US-Dollar"),
				Link("Fiskaljahr", "Geschäftsjahr"),
				Link("Softwareentwicklung", "Softwareentwicklung")),

			"Abraham Lincoln" -> List(
				// Article text links
				Link("President of the United States", "President of the United States", 29),

				// Template links
				Link("Hannibal Hamlin", "Hannibal Hamlin"),
				Link("Andrew Johnson", "Andrew Johnson"),
				Link("Tad", "Tad Lincoln"),
				Link("Mary Todd Lincoln", "Mary Todd Lincoln")),

			"Zerfall" -> List(
				// Disambiguation links
				Link("Zerfall", "Radioaktivität", -1),
				Link("Zerfall", "Zerfallsgesetz", -1),
				Link("Zerfall", "Exponentielles Wachstum", -1),
				Link("Zerfall", "Zerfall (Soziologie)", -1),
				Link("Zerfall", "Zerfall (Album)", -1),
				Link("Zerfall", "Raspad – Der Zerfall", -1)),

			"Fisch" -> List(
				// Disambiguation links
				Link("Fisch", "Fische", -1),
				Link("Fisch", "Speisefisch", -1),
				Link("Fisch", "Fische (Sternbild)", -1),
				Link("Fisch", "Fische (Tierkreiszeichen)", -1),
				Link("Fisch", "Fisch (Christentum)", -1),
				Link("Fisch", "Fisch (Wappentier)", -1),
				Link("Fisch", "Fisch (Saargau)", -1)),

			"Zwilling (Begriffsklärung)" -> List(
				// Disambiguation links
				Link("Zwilling", "Zwillinge", -1),
				Link("Zwilling", "Kristallzwilling", -1),
				Link("Zwilling", "Zwilling J. A. Henckels", -1),
				Link("Zwilling", "Zwillinge (Sternbild)", -1),
				Link("Zwilling", "Zwillinge (Tierkreiszeichen)", -1)))
	}

	def wikipediaTestTextLinks(): Map[String, List[Link]] = {
		// extracted text links from Article abstracts
		Map(
			"Audi" -> List(
				Link("Ingolstadt", "Ingolstadt", 55),
				Link("Bayern", "Bayern", 69),
				Link("Automobilhersteller", "Automobilhersteller", 94),
				Link("Volkswagen", "Volkswagen AG", 123),
				Link("Wortspiel", "Wortspiel", 175),
				Link("Namensrechte", "Marke (Recht)", 202),
				Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", 255),
				Link("August Horch", "August Horch", 316),
				Link("Lateinische", "Latein", 599),
				Link("Imperativ", "Imperativ (Modus)", 636),
				Link("Zwickau", "Zwickau", 829),
				Link("Zschopauer", "Zschopau", 868),
				Link("DKW", "DKW", 937),
				Link("Wanderer", "Wanderer (Unternehmen)", 1071),
				Link("Auto Union AG", "Auto Union", 1105),
				Link("Chemnitz", "Chemnitz", 1131),
				Link("Zweiten Weltkrieg", "Zweiter Weltkrieg", 1358),
				Link("Ingolstadt", "Ingolstadt", 1423),
				Link("NSU Motorenwerke AG", "NSU Motorenwerke", 1599),
				Link("Neckarsulm", "Neckarsulm", 1830)),

			"Electronic Arts" -> List(
				Link("Publisher", "Publisher", 83),
				Link("Computer- und Videospielen", "Computerspiel", 97),
				Link("Madden NFL", "Madden NFL", 180),
				Link("FIFA", "FIFA (Spieleserie)", 192),
				Link("Vivendi Games", "Vivendi Games", 346),
				Link("Activision", "Activision", 364),
				Link("Activision Blizzard", "Activision Blizzard", 378),
				Link("Nasdaq Composite", "Nasdaq Composite", 675),
				Link("S&P 500", "S&P 500", 699)),

			"Abraham Lincoln" -> List(
				Link("President of the United States", "President of the United States", 29)))
	}

	def wikipediaTestTemplateLinks(): Map[String, List[Link]] = {
		// extracted template links from Article abstracts
		Map(
			"Audi" -> List(
				Link("Aktiengesellschaft", "Aktiengesellschaft"),
				Link("Zwickau", "Zwickau"),
				Link("Chemnitz", "Chemnitz"),
				Link("Ingolstadt", "Ingolstadt"),
				Link("Neckarsulm", "Neckarsulm"),
				Link("Ingolstadt", "Ingolstadt"),
				Link("Deutschland", "Deutschland"),
				Link("Rupert Stadler", "Rupert Stadler"),
				Link("Vorstand", "Vorstand"),
				Link("Matthias Müller", "Matthias Müller (Manager)"),
				Link("Aufsichtsrat", "Aufsichtsrat"),
				Link("Mrd.", "Milliarde"),
				Link("EUR", "Euro"),
				Link("Automobilhersteller", "Automobilhersteller")),

			"Electronic Arts" -> List(
				Link("Corporation", "Gesellschaftsrecht der Vereinigten Staaten#Corporation"),
				Link("Redwood City", "Redwood City"),
				Link("USA", "Vereinigte Staaten"),
				Link("Larry Probst", "Larry Probst"),
				Link("USD", "US-Dollar"),
				Link("Fiskaljahr", "Geschäftsjahr"),
				Link("Softwareentwicklung", "Softwareentwicklung")),

			"Abraham Lincoln" -> List(
				Link("Hannibal Hamlin", "Hannibal Hamlin"),
				Link("Andrew Johnson", "Andrew Johnson"),
				Link("Tad", "Tad Lincoln"),
				Link("Mary Todd Lincoln", "Mary Todd Lincoln")))
	}

	// scalastyle:on method.length

	def wikipediaTestTemplateArticles(): Set[String] = {
		Set("Audi", "Electronic Arts", "Postbank-Hochhaus (Berlin)", "Abraham Lincoln")
	}

	def wikipediaEntriesTestList(): List[WikipediaEntry] = {
		// extracted from Wikipedia
		List(
			WikipediaEntry("Audi", Option("""{{Begriffsklärungshinweis}}\n{{Coordinate |NS=48/46/59.9808/N |EW=11/25/4.926/E |type=landmark |region=DE-BY }}\n{{Infobox Unternehmen\n| Name   = Audi AG\n| Logo   = Audi-Logo 2016.svg\n| Unternehmensform = [[Aktiengesellschaft]]\n| Gründungsdatum = 16. Juli 1909 in [[Zwickau]] (Audi)<br /><!--\n-->29. Juni 1932 in [[Chemnitz]] (Auto Union)<br /><!--\n-->3.&nbsp;September&nbsp;1949&nbsp;in&nbsp;[[Ingolstadt]]&nbsp;(Neugründung)<br /><!--\n-->10. März 1969 in [[Neckarsulm]] (Fusion)\n| ISIN   = DE0006757008\n| Sitz   = [[Ingolstadt]], [[Deutschland]]\n| Leitung  =\n* [[Rupert Stadler]],<br />[[Vorstand]]svorsitzender\n* [[Matthias Müller (Manager)|Matthias Müller]],<br />[[Aufsichtsrat]]svorsitzender\n| Mitarbeiterzahl = 82.838 <small>(31. Dez. 2015)</small><ref name="kennzahlen" />\n| Umsatz  = 58,42 [[Milliarde|Mrd.]] [[Euro|EUR]] <small>(2015)</small><ref name="kennzahlen" />\n| Branche  = [[Automobilhersteller]]\n| Homepage  = www.audi.de\n}}\n\n[[Datei:Audi Ingolstadt.jpg|mini|Hauptsitz in Ingolstadt]]\n[[Datei:Neckarsulm 20070725.jpg|mini|Audi-Werk in Neckarsulm (Bildmitte)]]\n[[Datei:Michèle Mouton, Audi Quattro A1 - 1983 (11).jpg|mini|Kühlergrill mit Audi-Emblem <small>[[Audi quattro]] (Rallye-Ausführung, Baujahr 1983)</small>]]\n[[Datei:Audi 2009 logo.svg|mini|Logo bis April 2016]]\n\nDie '''Audi AG''' ({{Audio|Audi AG.ogg|Aussprache}}, Eigenschreibweise: ''AUDI AG'') mit Sitz in [[Ingolstadt]] in [[Bayern]] ist ein deutscher [[Automobilhersteller]], der dem [[Volkswagen AG|Volkswagen]]-Konzern angehört.\n\nDer Markenname ist ein [[Wortspiel]] zur Umgehung der [[Marke (Recht)|Namensrechte]] des ehemaligen Kraftfahrzeugherstellers ''[[Horch|A. Horch & Cie. Motorwagenwerke Zwickau]]''. Unternehmensgründer [[August Horch]], der „seine“ Firma nach Zerwürfnissen mit dem Finanzvorstand verlassen hatte, suchte einen Namen für sein neues Unternehmen und fand ihn im Vorschlag des Zwickauer Gymnasiasten Heinrich Finkentscher (Sohn des mit A. Horch befreundeten Franz Finkentscher), der ''Horch'' ins [[Latein]]ische übersetzte.<ref>Film der Audi AG: ''Die Silberpfeile aus Zwickau.'' Interview mit August Horch, Video 1992.</ref> ''Audi'' ist der [[Imperativ (Modus)|Imperativ]] Singular von ''audire'' (zu Deutsch ''hören'', ''zuhören'') und bedeutet „Höre!“ oder eben „Horch!“. Am 25. April 1910 wurde die ''Audi Automobilwerke GmbH Zwickau'' in das Handelsregister der Stadt [[Zwickau]] eingetragen.\n\n1928 übernahm die [[Zschopau]]er ''Motorenwerke J. S. Rasmussen AG'', bekannt durch ihre Marke ''[[DKW]]'', die Audi GmbH. Audi wurde zur Tochtergesellschaft und 1932 mit der Übernahme der Horchwerke AG sowie einem Werk des Unternehmens ''[[Wanderer (Unternehmen)|Wanderer]]'' Teil der neu gegründeten ''[[Auto Union|Auto Union AG]]'' mit Sitz in [[Chemnitz]], die folglich die vier verschiedenen Marken unter einem Dach anboten. Daraus entstand auch das heutige aus vier Ringen bestehende Logo von Audi, das darin ursprünglich nur für einen der Ringe gestanden hatte.\n\nNach dem [[Zweiter Weltkrieg|Zweiten Weltkrieg]] wurde 1949 die ''Auto Union GmbH'' nun mit Sitz in [[Ingolstadt]] neugegründet. Nachdem diese sich zunächst auf die Marke ''DKW'' konzentriert hatte, wurde 1965 erstmals wieder die Marke ''Audi'' verwendet. Im Zuge der Fusion 1969 mit der ''[[NSU Motorenwerke|NSU Motorenwerke AG]]'' zur ''Audi NSU Auto Union AG'' wurde die Marke ''Audi'' zum ersten Mal nach 37 Jahren als prägender Bestandteil in den Firmennamen der Auto Union aufgenommen. Hauptsitz war, dem Fusionspartner entsprechend, bis 1985 in [[Neckarsulm]], bevor der Unternehmensname der ehemaligen Auto Union infolge des Auslaufens der Marke NSU auf ''Audi AG'' verkürzt wurde und der Sitz wieder zurück nach Ingolstadt wechselte.""")),
			WikipediaEntry("Electronic Arts", Option("""{{Infobox Unternehmen\n| Name             = Electronic Arts, Inc.\n| Logo             = [[Datei:Electronic-Arts-Logo.svg|200px]]\n| Unternehmensform = [[Gesellschaftsrecht der Vereinigten Staaten#Corporation|Corporation]]\n| ISIN             = US2855121099\n| Gründungsdatum   = 1982\n| Sitz             = [[Redwood City]], [[Vereinigte Staaten|USA]]\n| Leitung          = Andrew Wilson (CEO)<br />[[Larry Probst]] (Chairman)\n| Mitarbeiterzahl  = 9.300 (2013)<ref>Electronic Arts: [http://www.ea.com/about About EA]. Offizielle Unternehmenswebseite, zuletzt abgerufen am 31. Dezember 2013</ref>\n| Umsatz           = 3,575 Milliarden [[US-Dollar|USD]] <small>([[Geschäftsjahr|Fiskaljahr]] 2014)</small>\n| Branche          = [[Softwareentwicklung]]\n| Homepage         = [http://www.ea.com/de/ www.ea.com/de]\n}}\n'''Electronic Arts''' ('''EA''') ist ein börsennotierter, weltweit operierender Hersteller und [[Publisher]] von [[Computerspiel|Computer- und Videospielen]]. Das Unternehmen wurde vor allem für seine Sportspiele (''[[Madden NFL]]'', ''[[FIFA (Spieleserie)|FIFA]]'') bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von [[Vivendi Games]] und [[Activision]] zu [[Activision Blizzard]], war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt.<ref name="economist">''Looking forward to the next level. The world’s biggest games publisher sees good times ahead.'' In: ''The Economist.'' 8. Februar 2007, S.&nbsp;66.</ref> Die Aktien des Unternehmens sind im [[Nasdaq Composite]] und im [[S&P 500]] gelistet.""")),
			WikipediaEntry("Postbank-Hochhaus (Berlin)", Option("""{{Infobox Hohes Gebäude\n|Name=Postbank-Hochhaus Berlin\n|Bild=Postbank-Hochhaus Berlin.jpg|miniatur|Das Postbank-Hochhaus.\n|Ort=[[Berlin-Kreuzberg]]\n|Nutzung=Bürogebäude\n|Arbeitsplätze=\n|von=1965\n|bis=1971\n|Architekt=Prosper Lemoine\n|Baustil=[[Moderne (Architektur)|Moderne]]\n|Offizielle Höhe=89\n|Etagen=23\n|Fläche=\n|Geschossfläche=\n|Baustoffe=[[Stahlbeton]], [[Stahl]], Fassade aus [[Glas]]\n|Rang_Stadt=13\n|Rang_Land=\n|Stadt=Berlin\n|Land=Deutschland\n|Kontinent=Europa\n}}\n\nDas heutige '''Postbank-Hochhaus''' (früher: '''Postscheckamt Berlin West''' (Bln W), seit 1985: '''Postgiroamt Berlin''') ist ein [[Hochhaus]] der [[Postbank]] am [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Hallesches Ufer*|Halleschen Ufer]] 40–60 und der [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Großbeerenstraße*|Großbeerenstraße]] 2 im [[Berlin]]er Ortsteil [[Berlin-Kreuzberg|Kreuzberg]].\n\n== Geschichte und Entstehung ==\nDas Postscheckamt von Berlin war ab 1909 in einem Neubau in der [[Dorotheenstraße (Berlin)|Dorotheenstraße]] 29 (heute: 84), der einen Teil der ehemaligen [[Markthalle IV]] integrierte, untergebracht und war bis zum Ende des [[Zweiter Weltkrieg|Zweiten Weltkriegs]] für den Bereich der Städte Berlin, [[Frankfurt (Oder)]], [[Potsdam]], [[Magdeburg]] und [[Stettin]] zuständig. Aufgrund der [[Deutsche Teilung|Deutschen Teilung]] wurde das Postscheckamt in der Dorotheenstraße nur noch von der [[Deutsche Post (DDR)|Deutschen Post der DDR]] genutzt. Für den [[West-Berlin|Westteil von Berlin]] gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das ''Postscheckamt West'' eröffnet. 2014 kaufte die ''CG-Gruppe'' das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden.<ref>[http://www.berliner-zeitung.de/berlin/kreuzberg-wohnen-im-postbank-tower-3269104 ''Kreuzberg: Wohnen im Postbank-Tower''.] In: ''[[Berliner Zeitung]]'', 7. Februar 2014</ref>\n\n== Architektur ==\n[[Datei:Gottfried Gruner - Springbrunnen.jpg|mini|Springbrunnenanlage von [[Gottfried Gruner]]]]\n\nNach den Plänen des Oberpostdirektors [[Prosper Lemoine]] wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23&nbsp;[[Geschoss (Architektur)|Geschosse]] und gehört mit einer Höhe von 89&nbsp;Metern bis heute zu den [[Liste der Hochhäuser in Berlin|höchsten Gebäuden in Berlin]]. Das Hochhaus besitzt eine [[Aluminium]]-Glas-Fassade und wurde im sogenannten „[[Internationaler Stil|Internationalen Stil]]“ errichtet. Die Gestaltung des Gebäudes orientiert sich an [[Mies van der Rohe]]s [[Seagram Building]] in [[New York City|New York]].\n\nZu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der ''Große Brunnen'' von [[Gottfried Gruner]]. Er besteht aus 18&nbsp;Säulen aus [[Bronze]] und wurde 1972 in Betrieb genommen.\n\n== UKW-Sender ==\nIm Postbank-Hochhaus befinden sich mehrere [[Ultrakurzwellensender|UKW-Sender]], die von [[Media Broadcast]] betrieben werden. Die [[Deutsche Funkturm]] (DFMG), eine Tochtergesellschaft der [[Deutsche Telekom|Deutschen Telekom AG]], stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u.&nbsp;a. folgende Hörfunkprogramme auf [[Ultrakurzwelle]] ausgestrahlt:\n* [[88vier]], 500-W-Sender auf 88,4 MHz\n* [[NPR Berlin]], 400-W-Sender auf 104,1 MHz\n* [[Radio Russkij Berlin]], 100-W-Sender auf 97,2 MHz\n\n== Siehe auch ==\n* [[Postscheckamt]]\n* [[Postgeschichte und Briefmarken Berlins]]\n* [[Liste von Sendeanlagen in Berlin]]\n\n== Literatur ==\n* ''Vom Amt aus gesehen – Postscheckamt Berlin West (Prosper Lemoine).'' In: ''[[Bauwelt (Zeitschrift)|Bauwelt]].'' 43/1971 (Thema: Verwaltungsgebäude).\n\n== Weblinks ==\n{{Commons|Postbank (Berlin)}}\n* [http://www.luise-berlin.de/lexikon/frkr/p/postgiroamt.htm Postscheckamt Berlin West auf der Seite des Luisenstädter Bildungsvereins]\n* [http://www.bildhauerei-in-berlin.de/_html/_katalog/details-1445.html Bildhauerei in Berlin: Vorstellung des großen Brunnens]\n\n== Einzelnachweise ==\n<references />\n\n{{Coordinate|article=/|NS=52.499626|EW=13.383655|type=landmark|region=DE-BE}}\n\n{{SORTIERUNG:Postbank Hochhaus Berlin}}\n[[Kategorie:Berlin-Kreuzberg]]\n[[Kategorie:Erbaut in den 1970er Jahren]]\n[[Kategorie:Bürogebäude in Berlin]]\n[[Kategorie:Bauwerk der Moderne in Berlin]]\n[[Kategorie:Berlin-Mitte]]\n[[Kategorie:Hochhaus in Berlin]]\n[[Kategorie:Postgeschichte (Deutschland)]]\n[[Kategorie:Landwehrkanal]]\n[[Kategorie:Hochhaus in Europa]]""")),
			WikipediaEntry("Postbank-Hochhaus Berlin", Option("""#WEITERLEITUNG [[Postbank-Hochhaus (Berlin)]]""")),
			WikipediaEntry("Abraham Lincoln", Option("""{{Infobox officeholder\n| vicepresident = [[Hannibal Hamlin]]\n}}\nAbraham Lincoln was the 16th [[President of the United States]].\n{{Infobox U.S. Cabinet\n| Vice President 2 = [[Andrew Johnson]]\n}}\n{{multiple image\n | direction=horizontal\n | width=\n | footer=\n | width1=180\n | image1=A&TLincoln.jpg\n | alt1=A seated Lincoln holding a book as his young son looks at it\n | caption1=1864 photo of President Lincoln with youngest son, [[Tad Lincoln|Tad]]\n | width2=164\n | image2=Mary Todd Lincoln 1846-1847 restored cropped.png\n | alt2=Black and white photo of Mary Todd Lincoln's shoulders and head\n | caption2=[[Mary Todd Lincoln]], wife of Abraham Lincoln, age 28\n }}""")),
			WikipediaEntry("Zerfall", Option("""'''Zerfall''' steht für:\n* Radioaktiver Zerfall, siehe [[Radioaktivität]]\n* das [[Zerfallsgesetz]] einer radioaktiven Substanz\n* Exponentieller Zerfall, siehe [[Exponentielles Wachstum]]\n* den [[Zerfall (Soziologie)|Zerfall]] gesellschaftlicher Strukturen \n* ein Album der Band Eisregen, siehe [[Zerfall (Album)]]\n* den Film [[Raspad – Der Zerfall]]\n\n'''Siehe auch:'''\n{{Wiktionary}}\n\n{{Begriffsklärung}}""")),
			WikipediaEntry("Fisch", Option("""'''Fisch''' steht für:\n\n* [[Fische]], im Wasser lebende Wirbeltiere \n* [[Speisefisch]], eine Lebensmittelkategorie\n* [[Fische (Sternbild)]]\n* [[Fische (Tierkreiszeichen)]]\n* [[Fisch (Christentum)]], religiöses Symbol\n* [[Fisch (Wappentier)]], gemeine Figur in der Heraldik\n\n'''Fisch''' ist der Name folgender Orte:\n\n* [[Fisch (Saargau)]], Ortsgemeinde in Rheinland-Pfalz\n\n{{Begriffsklärung}}""")),
			WikipediaEntry("Zwilling (Begriffsklärung)", Option("""'''Zwilling''' bezeichnet:\n\n* den biologischen Zwilling, siehe [[Zwillinge]]\n* einen Begriff aus der Kristallkunde, siehe [[Kristallzwilling]]\n* [[Zwilling J. A. Henckels]], einen Hersteller von Haushaltswaren mit Sitz in Solingen\n\nin der Astronomie und Astrologie:\n\n* ein Sternbild, siehe [[Zwillinge (Sternbild)]]\n* eines der Tierkreiszeichen, siehe [[Zwillinge (Tierkreiszeichen)]]\n\n{{Begriffsklärung}}"""))
		)
	}

	def wikipediaDisambiguationPagesTestSet(): Set[String] = {
		Set("Zerfall", "Fisch", "Zwilling (Begriffsklärung)")
	}

	def wikipediaTestAbstracts(): Map[String, String] = {
		Map(
			"Audi" -> """Die Audi AG (, Eigenschreibweise: AUDI AG) mit Sitz in Ingolstadt in Bayern ist ein deutscher Automobilhersteller, der dem Volkswagen-Konzern angehört. Der Markenname ist ein Wortspiel zur Umgehung der Namensrechte des ehemaligen Kraftfahrzeugherstellers A. Horch & Cie. Motorwagenwerke Zwickau.""",
			"Electronic Arts" -> """Electronic Arts (EA) ist ein börsennotierter, weltweit operierender Hersteller und Publisher von Computer- und Videospielen. Das Unternehmen wurde vor allem für seine Sportspiele (Madden NFL, FIFA) bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von Vivendi Games und Activision zu Activision Blizzard, war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt. Die Aktien des Unternehmens sind im Nasdaq Composite und im S&P 500 gelistet.""")
	}

	def groupedAliasesTestSet(): Set[Alias] = {
		Set(
			Alias("Ingolstadt", Map("Ingolstadt" -> 1)),
			Alias("Bayern", Map("Bayern" -> 1)),
			Alias("Automobilhersteller", Map("Automobilhersteller" -> 1)),
			Alias("Zerfall", Map("Zerfall (Album)" -> 1, "Zerfall (Soziologie)" -> 1))
		)
	}

	def groupedPagesTestSet(): Set[Page] = {
		Set(
			Page("Ingolstadt", Map("Ingolstadt" -> 1)),
			Page("Bayern", Map("Bayern" -> 1)),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1)),
			Page("Zerfall (Album)", Map("Zerfall" -> 1)),
			Page("Zerfall (Soziologie)", Map("Zerfall" -> 1))
		)
	}

	def cleanedGroupedAliasesTestSet(): Set[Alias] = {
		Set(
			Alias("Ingolstadt", Map("Ingolstadt" -> 1)),
			Alias("Bayern", Map("Bayern" -> 1)),
			Alias("Automobilhersteller", Map("Automobilhersteller" -> 1)),
			Alias("Zerfall", Map("Zerfall (Album)" -> 1))
		)
	}

	def cleanedGroupedPagesTestSet(): Set[Page] = {
		Set(
			Page("Ingolstadt", Map("Ingolstadt" -> 1)),
			Page("Bayern", Map("Bayern" -> 1)),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1)),
			Page("Zerfall (Album)", Map("Zerfall" -> 1))
		)
	}

	def probabilityReferences(): Map[String, Double] = {
		Map(
			"Ingolstadt" -> 0.0,
			"Bayern" -> 1.0,
			"Automobilhersteller" -> 0.0,
			"Zerfall" -> 0.0
		)
	}

	def allPagesTestList(): List[String] = {
		List(
			"Automobilhersteller",
			"Ingolstadt",
			"Bayern",
			"Zerfall (Album)"
		)
	}

	def smallerParsedWikipediaTestList(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry("Audi", Option("dummy text"), List(
				Link("Ingolstadt", "Ingolstadt", 55),
				Link("Bayern", "Bayern", 69),
				Link("Automobilhersteller", "Automobilhersteller", 94),
				Link("Zerfall", "Zerfall (Album)", 4711),
				Link("Zerfall", "Zerfall (Soziologie)", 4711), // dead link
				Link("", "page name with empty alias", 4711),
				Link("alias with empty page name", "", 4711)
			),
				List()
			))
	}

	def germanStopwordsTestSet(): Set[String] = {
		Set("der", "die", "das", "und", "als", "ist", "an", "am", "im", "dem", "des")
	}

	def unstemmedDFTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("Audi", 3),
			DocumentFrequency("Backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1),
			DocumentFrequency("einer", 2),
			DocumentFrequency("ein", 2),
			DocumentFrequency("Einer", 1),
			DocumentFrequency("Ein", 1)
		)
	}

	def stemmedDFTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1),
			DocumentFrequency("ein", 6)
		)
	}

	def unstemmedGermanWordsTestList(): List[String] = {
		List("Es", "Keine", "Kein", "Keiner", "Ist", "keine", "keiner", "Streitberg", "Braunschweig",
			"Deutschland", "Baum", "brauchen", "suchen", "könnte")
		// problem words: eine -> eine, hatte -> hatt
	}

	def stemmedGermanWordsTestList(): List[String] = {
		List("es", "kein", "kein", "kein", "ist", "kein", "kein", "streitberg", "braunschweig",
			"deutschla", "baum", "brauch", "such", "konn")
	}

	def parsedTestEntry(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Schwarzer Humor",
			Option("""Hickelkasten in Barcelona, Spanien: Der Sprung in den „Himmel“ ist in diesem Fall ein Sprung in den Tod. Hier hat sich jemand einen makabren Scherz erlaubt. Als schwarzer Humor wird Humor bezeichnet, der Verbrechen, Krankheit, Tod und ähnliche Themen, für die gewöhnlich eine Abhandlung in ernster Form erwartet wird, in satirischer oder bewusst verharmlosender Weise verwendet. Oft bezieht er sich auf Zeitthemen. Schwarzer Humor bedient sich häufig paradoxer Stilfiguren. Nicht selten löst er Kontroversen aus darüber, ob man sich über die genannten Dinge lustig machen dürfe und wo die Grenzen des guten Geschmacks lägen; besonders ist dies der Fall, wenn religiöse und sexuelle Themen und tragische Ereignisse zum Gegenstand genommen werden. In der darstellenden Kunst nennt man auf schwarzen Humor setzende Werke schwarze Komödien. Der Begriff wurde durch den Surrealisten André Breton erstmals 1940 in seiner Schrift Anthologie de l’humour noir näher umrissen, wird jedoch seit den 1960er Jahren zum Teil deutlich anders verstanden, indem Kennzeichen der Desillusion und des Nihilismus hinzutraten. In dem Vorwort seines Werkes nennt Breton unter anderem Quellen von Freud und Hegel, die seiner Meinung nach in die Begriffsentwicklung eingeflossen sind. Ursprünge des ‚schwarzen Humors‘ sah Breton in seiner Anthologie bei einigen Werken des irischen Satirikers Jonathan Swift wie Directions to Servants, A Modest Proposal, A Meditation on a Broom-Stick und einige seiner Aphorismen. In den öffentlichen Gebrauch kam der Begriff erst in den 1960er Jahren insbesondere im angloamerikanischen Raum (‚black humour‘) durch die Rezeption von Schriftstellern wie Nathanael West, Vladimir Nabokov und Joseph Heller. So gilt Catch-22 (1961) als ein bekanntes Beispiel dieser Stilart, in dem die Absurdität des Militarismus im Zweiten Weltkrieg satirisch überspitzt wurde. Weitere Beispiele sind Kurt Vonnegut, Slaughterhouse Five (1969), Thomas Pynchon, V. (1963) und Gravity’s Rainbow (1973), sowie im Film Stanley Kubrick’s Dr. Strangelove (1964) und im Absurden Theater insbesondere bei Eugène Ionesco zu finden. Der Begriff black comedy (dtsch. „schwarze Komödie“), der in der englischen Sprache schon für einige Stücke Shakespeares angewandt wurde, weist nach dem Lexikon der Filmbegriffe der Christian-Albrechts-Universität zu Kiel als Komödientyp durch „manchmal sarkastischen, absurden und morbiden ‚schwarzen‘ Humor“ aus, der sich sowohl auf „ernste oder tabuisierte Themen wie Krankheit, Behinderung, Tod, Krieg, Verbrechen“ wie auch auf „für sakrosankt gehaltene Dinge“ richten kann und dabei „auch vor politischen Unkorrektheiten, derben Späßen, sexuellen und skatologischen Anzüglichkeiten nicht zurückschreckt.“ Dabei stehe „hinter der Fassade zynischer Grenzüberschreitungen“ häufig ein „aufrichtiges Anliegen, falsche Hierarchien, Konventionen und Verlogenheiten innerhalb einer Gesellschaft mit den Mitteln filmischer Satire zu entlarven.“ Als filmische Beispiele werden angeführt: Robert Altmans M*A*S*H (USA 1970), Mike Nichols’ Catch-22 (USA 1970), nach Joseph Heller) sowie in der Postmoderne Quentin Tarantinos Pulp Fiction (USA 1994) und Lars von Triers Idioterne (Dänemark 1998). Der Essayist François Bondy schrieb 1971 in Die Zeit: „Der schwarze Humor ist nicht zu verwechseln mit dem ‚kranken Humor‘, der aus den Staaten kam, mit seinen ‚sick jokes‘“ und nannte als Beispiel den Witz: „Mama, ich mag meinen kleinen Bruder nicht. – Schweig, iß, was man dir vorsetzt“. Witz und Humor seien jedoch nicht dasselbe und letzteres „eine originale Geschichte in einer besonderen Tonart“. Humor im Sinne von einer – wie der Duden definiert – „vorgetäuschten Heiterkeit mit der jemand einer unangenehmen oder verzweifelten Lage, in der er sich befindet, zu begegnen“ versucht, nennt man auch Galgenhumor."""),
			List[Link](Link("Hickelkasten", "Hickelkasten", 0), Link("Humor", "Humor", 182), Link("satirischer", "Satire", 321), Link("paradoxer", "Paradoxon", 451), Link("Stilfiguren", "Rhetorische Figur", 461), Link("Kontroversen", "Kontroverse", 495), Link("guten Geschmacks", "Geschmack (Kultur)", 601), Link("Surrealisten", "Surrealismus", 865), Link("André Breton", "André Breton", 878), Link("Desillusion", "Desillusion", 1061), Link("Nihilismus", "Nihilismus", 1081), Link("Freud", "Sigmund Freud", 1173), Link("Hegel", "Georg Wilhelm Friedrich Hegel", 1183), Link("Anthologie", "Anthologie", 1314), Link("Jonathan Swift", "Jonathan Swift", 1368), Link("Directions to Servants", "Directions to Servants", 1387), Link("A Modest Proposal", "A Modest Proposal", 1411), Link("A Meditation on a Broom-Stick", "A Meditation on a Broom-Stick", 1430), Link("Aphorismen", "Aphorismus", 1478), Link("Nathanael West", "Nathanael West", 1663), Link("Vladimir Nabokov", "Vladimir Nabokov", 1679), Link("Joseph Heller", "Joseph Heller", 1700), Link("Catch-22", "Catch-22", 1723), Link("Kurt Vonnegut", "Kurt Vonnegut", 1893), Link("Slaughterhouse Five", "Schlachthof 5 oder Der Kinderkreuzzug", 1908), Link("Thomas Pynchon", "Thomas Pynchon", 1936), Link("V.", "V.", 1952), Link("Gravity’s Rainbow", "Die Enden der Parabel", 1966), Link("Stanley Kubrick", "Stanley Kubrick", 2006), Link("Dr. Strangelove", "Dr. Seltsam oder: Wie ich lernte, die Bombe zu lieben", 2024), Link("Absurden Theater", "Absurdes Theater", 2054), Link("Eugène Ionesco", "Eugène Ionesco", 2088), Link("Shakespeares", "Shakespeare", 2222), Link("Christian-Albrechts-Universität zu Kiel", "Christian-Albrechts-Universität zu Kiel", 2296), Link("Komödientyp", "Komödie", 2340), Link("sarkastischen", "Sarkasmus", 2368), Link("absurden", "Absurdität", 2383), Link("morbiden", "Morbidität", 2396), Link("tabuisierte", "Tabuisierung", 2462), Link("sakrosankt", "Sakrosankt", 2551), Link("politischen Unkorrektheiten", "Politische Korrektheit", 2612), Link("sexuellen und skatologischen", "Vulgärsprache", 2656), Link("zynischer", "Zynismus", 2756), Link("Satire", "Satire", 2933), Link("Robert Altmans", "Robert Altman", 2997), Link("M*A*S*H", "MASH (Film)", 3012), Link("Mike Nichols", "Mike Nichols", 3032), Link("Catch-22", "Catch-22 – Der böse Trick", 3046), Link("Joseph Heller", "Joseph Heller", 3071), Link("Postmoderne", "Postmoderne", 3099), Link("Quentin Tarantinos", "Quentin Tarantino", 3111), Link("Pulp Fiction", "Pulp Fiction", 3130), Link("Lars von Triers", "Lars von Trier", 3158), Link("Idioterne", "Idioten", 3174), Link("François Bondy", "François Bondy", 3214), Link("Die Zeit", "Die Zeit", 3245), Link("Witz", "Witz", 3491), Link("Duden", "Duden", 3639), Link("Galgenhumor", "Galgenhumor", 3806)),
			List())
	}

	def testNamespacePages(): List[WikipediaEntry] = {
		List(
			WikipediaEntry("lol"),
			WikipediaEntry("Audi"),
			WikipediaEntry("Electronic Arts"),
			WikipediaEntry("Postbank-Hochhaus (Berlin)"),
			WikipediaEntry("Postbank-Hochhaus Berlin"),
			WikipediaEntry("Abraham Lincoln"),
			WikipediaEntry("Diskussion:Testpage"),
			WikipediaEntry("Benutzer:Testpage"),
			WikipediaEntry("Benutzerin:Testpage"),
			WikipediaEntry("Benutzer Diskussion:Testpage"),
			WikipediaEntry("BD:Testpage"),
			WikipediaEntry("Benutzerin Diskussion:Testpage"),
			WikipediaEntry("Wikipedia:Testpage"),
			WikipediaEntry("WP:Testpage"),
			WikipediaEntry("Wikipedia Diskussion:Testpage"),
			WikipediaEntry("WD:Testpage"),
			WikipediaEntry("Datei:Testpage"),
			WikipediaEntry("Datei Diskussion:Testpage"),
			WikipediaEntry("MediaWiki:Testpage"),
			WikipediaEntry("MediaWiki Diskussion:Testpage"),
			WikipediaEntry("Vorlage:Testpage"),
			WikipediaEntry("Vorlage Diskussion:Testpage"),
			WikipediaEntry("Hilfe:Testpage"),
			WikipediaEntry("H:Testpage"),
			WikipediaEntry("Hilfe Diskussion:Testpage"),
			WikipediaEntry("HD:Testpage"),
			WikipediaEntry("Kategorie:Testpage"),
			WikipediaEntry("Kategorie Diskussion:Testpage"),
			WikipediaEntry("Portal:Testpage"),
			WikipediaEntry("P:Testpage"),
			WikipediaEntry("Portal Diskussion:Testpage"),
			WikipediaEntry("PD:Testpage"),
			WikipediaEntry("Modul:Testpage"),
			WikipediaEntry("Modul Diskussion:Testpage"),
			WikipediaEntry("Gadget:Testpage"),
			WikipediaEntry("Gadget Diskussion:Testpage"),
			WikipediaEntry("Gadget-Definition:Testpage"),
			WikipediaEntry("Gadget-Definition Diskussion:Testpage"),
			WikipediaEntry("Thema:Testpage"),
			WikipediaEntry("Spezial:Testpage"),
			WikipediaEntry("Medium:Testpage"))
	}

	def testCleanedNamespacePages(): List[WikipediaEntry] = {
		List(
			WikipediaEntry("lol"),
			WikipediaEntry("Audi"),
			WikipediaEntry("Electronic Arts"),
			WikipediaEntry("Postbank-Hochhaus (Berlin)"),
			WikipediaEntry("Postbank-Hochhaus Berlin"),
			WikipediaEntry("Abraham Lincoln"),
			WikipediaEntry("Kategorie:Testpage"))
	}

	def testNamespaceLinks(): List[Link] = {
		List(
			Link("August Horch", "August Horch", 0),
			Link("August Horch", "Benutzer:August Horch", 0),
			Link("August Horch", "Thema:August Horch", 0),
			Link("August Horch", "HD:August Horch", 0),
			Link("August Horch", "Kategorie:August Horch", 0)
		)
	}

	def testListLinkPage(): String = {
		Source.fromURL(getClass.getResource("/test_data.html")).getLines().mkString("\n")
	}

	def testExtractedListLinks(): List[Link] = {
		List(
			Link("Zwillinge", "Zwillinge"),
			Link("Kristallzwilling", "Kristallzwilling"),
			Link("Zwilling J. A. Henckels", "Zwilling J. A. Henckels"),
			Link("Zwillinge (Sternbild)", "Zwillinge (Sternbild)"),
			Link("Zwillinge (Tierkreiszeichen)", "Zwillinge (Tierkreiszeichen)"),
			Link("Christian Zwilling", "Christian Zwilling"),
			Link("David Zwilling", "David Zwilling"),
			Link("Edgar Zwilling", "Edgar Zwilling"),
			Link("Ernst Zwilling", "Ernst Zwilling"),
			Link("Gabriel Zwilling", "Gabriel Zwilling"),
			Link("Michail Jakowlewitsch Zwilling", "Michail Jakowlewitsch Zwilling"),
			Link("Paul Zwilling", "Paul Zwilling"),
			Link("Georg Zwilling", "Georg Zwilling"),
			Link("Poker", "Poker"),
			Link("Primzahlzwilling", "Primzahlzwilling"),
			Link("Zwilling (Heeresfeldbahn)", "Zwilling (Heeresfeldbahn)"),
			Link("Ivan Reitman", "Ivan Reitman"),
			Link("Twins - Zwillinge", "Twins - Zwillinge"),
			Link("Zwillingswendeltreppe", "Zwillingswendeltreppe"),
			Link("Der Zwilling", "Der Zwilling"),
			Link("Die Zwillinge", "Die Zwillinge"))
	}

	def testCleanedNamespaceLinks(): List[Link] = {
		List(
			Link("August Horch", "August Horch", 0),
			Link("August Horch", "Kategorie:August Horch", 0)
		)
	}

	def testCategoryLinks(): List[Link] = {
		List(
			Link("Ingolstadt", "Ingolstadt", 55),
			Link("Bayern", "Bayern", 69),
			Link("Automobilhersteller", "Kategorie:Automobilhersteller", 94),
			Link("Volkswagen", "Volkswagen AG", 123),
			Link("Wortspiel", "Kategorie:Wortspiel", 175),
			Link("Kategorie:Namensrechte", "Marke (Recht)", 202),
			Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", 255),
			Link("August Horch", "August Horch", 316),
			Link("Lateinische", "Latein", 599),
			Link("Imperativ", "Imperativ (Modus)", 636),
			Link("Kategorie:Zwickau", "Zwickau", 829),
			Link("Zschopauer", "Zschopau", 868))
	}

	def testCleanedCategoryLinks(): List[Link] = {
		List(
			Link("Ingolstadt", "Ingolstadt", 55),
			Link("Bayern", "Bayern", 69),
			Link("Volkswagen", "Volkswagen AG", 123),
			Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", 255),
			Link("August Horch", "August Horch", 316),
			Link("Lateinische", "Latein", 599),
			Link("Imperativ", "Imperativ (Modus)", 636),
			Link("Zschopauer", "Zschopau", 868))
	}

	def testExtractedCategoryLinks(): List[Link] = {
		List(
			Link("Automobilhersteller", "Automobilhersteller", 0),
			Link("Wortspiel", "Wortspiel", 0),
			Link("Namensrechte", "Marke (Recht)", 0),
			Link("Zwickau", "Zwickau", 0))
	}

	def testRedirectCornerCaseEntries(): Set[ParsedWikipediaEntry] = {
		Set(ParsedWikipediaEntry("?", Option("#WEITERLEITUNG [[?]]"), List[Link](Link("?", "?", 0))))
	}

	def testRedirectDict(): Map[String, String] = {
		Map("Postbank-Hochhaus Berlin" -> "Postbank-Hochhaus (Berlin)")
	}

	def testLinksWithRedirects(): Set[Link] = {
		Set(Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus Berlin", 10))
	}

	def testLinksWithResolvedRedirects(): Set[Link] = {
		Set(Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus (Berlin)", 10))
	}

	def testEntriesWithBadRedirects(): List[WikipediaEntry] = {
		List(WikipediaEntry("Postbank-Hochhaus Berlin", Option("""#redirect [[Postbank-Hochhaus (Berlin)]]""")))
	}

	// scalastyle:on line.size.limit
}

// scalastyle:on number.of.methods
