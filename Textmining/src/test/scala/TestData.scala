import WikiClasses._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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

	def parsedWikipediaTestRDD(sc: SparkContext): RDD[ParsedWikipediaEntry] = {
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

	def aliasOccurrencesInArticlesTestRDD(sc: SparkContext): RDD[AliasOccurrencesInArticle] = {
		sc.parallelize(List(
			AliasOccurrencesInArticle(Set("Audi"), Set()),
			AliasOccurrencesInArticle(Set(), Set("Audi")),
			AliasOccurrencesInArticle(Set("Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"), Set("Streitberg")),
			AliasOccurrencesInArticle(Set("Audi", "Brachttal", "historisches Jahr"), Set("Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"))
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
			.sortBy(_.alias)
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

	def getArticle(title: String, sc: SparkContext): ParsedWikipediaEntry = {
		parsedWikipediaTestRDD(sc)
			.filter(_.title == title)
			.first
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
				"Main-Kinzig-Kreis","Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen"),
			"Streitberg (Brachttal)" ->
				Set("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde",
					"Im", "Jahre", "1500", "ist", "von", "Stridberg", "die", "Vom", "Mittelalter",
					"bis", "ins", "Jahrhundert", "hatte", "der", "Ort", "Waldrechte"))
	}

	def documentFrequenciesTestRDD(sc: SparkContext): RDD[DocumentFrequency] = {
		sc.parallelize(List(
			DocumentFrequency("Audi", 3),
			DocumentFrequency("Backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1)
		))
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
				Link("Aktiengesellschaft", "Aktiengesellschaft", WikipediaTextparser.templateOffset),
				Link("Zwickau", "Zwickau", WikipediaTextparser.templateOffset),
				Link("Chemnitz", "Chemnitz", WikipediaTextparser.templateOffset),
				Link("Ingolstadt", "Ingolstadt", WikipediaTextparser.templateOffset),
				Link("Neckarsulm", "Neckarsulm", WikipediaTextparser.templateOffset),
				Link("Ingolstadt", "Ingolstadt", WikipediaTextparser.templateOffset),
				Link("Deutschland", "Deutschland", WikipediaTextparser.templateOffset),
				Link("Rupert Stadler", "Rupert Stadler", WikipediaTextparser.templateOffset),
				Link("Vorstand", "Vorstand", WikipediaTextparser.templateOffset),
				Link("Matthias Müller", "Matthias Müller (Manager)", WikipediaTextparser.templateOffset),
				Link("Aufsichtsrat", "Aufsichtsrat", WikipediaTextparser.templateOffset),
				Link("Mrd.", "Milliarde", WikipediaTextparser.templateOffset),
				Link("EUR", "Euro", WikipediaTextparser.templateOffset),
				Link("Automobilhersteller", "Automobilhersteller", WikipediaTextparser.templateOffset)),

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
				Link("Corporation", "Gesellschaftsrecht der Vereinigten Staaten#Corporation", WikipediaTextparser.templateOffset),
				Link("Redwood City", "Redwood City", WikipediaTextparser.templateOffset),
				Link("USA", "Vereinigte Staaten", WikipediaTextparser.templateOffset),
				Link("Larry Probst", "Larry Probst", WikipediaTextparser.templateOffset),
				Link("USD", "US-Dollar", WikipediaTextparser.templateOffset),
				Link("Fiskaljahr", "Geschäftsjahr", WikipediaTextparser.templateOffset),
				Link("Softwareentwicklung", "Softwareentwicklung", WikipediaTextparser.templateOffset)),

			"Abraham Lincoln" -> List(
				// Article text links
				Link("President of the United States", "President of the United States", 29),

				// Template links
				Link("Hannibal Hamlin", "Hannibal Hamlin", WikipediaTextparser.templateOffset),
				Link("Andrew Johnson", "Andrew Johnson", WikipediaTextparser.templateOffset),
				Link("Tad", "Tad Lincoln", WikipediaTextparser.templateOffset),
				Link("Mary Todd Lincoln", "Mary Todd Lincoln", WikipediaTextparser.templateOffset)))
	}
	// scalastyle:on method.length

	def wikipediaTestTemplateArticles(): Set[String] = {
		Set("Audi", "Electronic Arts", "Postbank-Hochhaus (Berlin)", "Abraham Lincoln")
	}

	def wikipediaTestRDD(sc: SparkContext): RDD[WikipediaEntry] = {
		// extracted from Wikipedia
		sc.parallelize(List(
			WikipediaEntry("Audi", Option("""{{Begriffsklärungshinweis}}\n{{Coordinate |NS=48/46/59.9808/N |EW=11/25/4.926/E |type=landmark |region=DE-BY }}\n{{Infobox Unternehmen\n| Name   = Audi AG\n| Logo   = Audi-Logo 2016.svg\n| Unternehmensform = [[Aktiengesellschaft]]\n| Gründungsdatum = 16. Juli 1909 in [[Zwickau]] (Audi)<br /><!--\n-->29. Juni 1932 in [[Chemnitz]] (Auto Union)<br /><!--\n-->3.&nbsp;September&nbsp;1949&nbsp;in&nbsp;[[Ingolstadt]]&nbsp;(Neugründung)<br /><!--\n-->10. März 1969 in [[Neckarsulm]] (Fusion)\n| ISIN   = DE0006757008\n| Sitz   = [[Ingolstadt]], [[Deutschland]]\n| Leitung  =\n* [[Rupert Stadler]],<br />[[Vorstand]]svorsitzender\n* [[Matthias Müller (Manager)|Matthias Müller]],<br />[[Aufsichtsrat]]svorsitzender\n| Mitarbeiterzahl = 82.838 <small>(31. Dez. 2015)</small><ref name="kennzahlen" />\n| Umsatz  = 58,42 [[Milliarde|Mrd.]] [[Euro|EUR]] <small>(2015)</small><ref name="kennzahlen" />\n| Branche  = [[Automobilhersteller]]\n| Homepage  = www.audi.de\n}}\n\n[[Datei:Audi Ingolstadt.jpg|mini|Hauptsitz in Ingolstadt]]\n[[Datei:Neckarsulm 20070725.jpg|mini|Audi-Werk in Neckarsulm (Bildmitte)]]\n[[Datei:Michèle Mouton, Audi Quattro A1 - 1983 (11).jpg|mini|Kühlergrill mit Audi-Emblem <small>[[Audi quattro]] (Rallye-Ausführung, Baujahr 1983)</small>]]\n[[Datei:Audi 2009 logo.svg|mini|Logo bis April 2016]]\n\nDie '''Audi AG''' ({{Audio|Audi AG.ogg|Aussprache}}, Eigenschreibweise: ''AUDI AG'') mit Sitz in [[Ingolstadt]] in [[Bayern]] ist ein deutscher [[Automobilhersteller]], der dem [[Volkswagen AG|Volkswagen]]-Konzern angehört.\n\nDer Markenname ist ein [[Wortspiel]] zur Umgehung der [[Marke (Recht)|Namensrechte]] des ehemaligen Kraftfahrzeugherstellers ''[[Horch|A. Horch & Cie. Motorwagenwerke Zwickau]]''. Unternehmensgründer [[August Horch]], der „seine“ Firma nach Zerwürfnissen mit dem Finanzvorstand verlassen hatte, suchte einen Namen für sein neues Unternehmen und fand ihn im Vorschlag des Zwickauer Gymnasiasten Heinrich Finkentscher (Sohn des mit A. Horch befreundeten Franz Finkentscher), der ''Horch'' ins [[Latein]]ische übersetzte.<ref>Film der Audi AG: ''Die Silberpfeile aus Zwickau.'' Interview mit August Horch, Video 1992.</ref> ''Audi'' ist der [[Imperativ (Modus)|Imperativ]] Singular von ''audire'' (zu Deutsch ''hören'', ''zuhören'') und bedeutet „Höre!“ oder eben „Horch!“. Am 25. April 1910 wurde die ''Audi Automobilwerke GmbH Zwickau'' in das Handelsregister der Stadt [[Zwickau]] eingetragen.\n\n1928 übernahm die [[Zschopau]]er ''Motorenwerke J. S. Rasmussen AG'', bekannt durch ihre Marke ''[[DKW]]'', die Audi GmbH. Audi wurde zur Tochtergesellschaft und 1932 mit der Übernahme der Horchwerke AG sowie einem Werk des Unternehmens ''[[Wanderer (Unternehmen)|Wanderer]]'' Teil der neu gegründeten ''[[Auto Union|Auto Union AG]]'' mit Sitz in [[Chemnitz]], die folglich die vier verschiedenen Marken unter einem Dach anboten. Daraus entstand auch das heutige aus vier Ringen bestehende Logo von Audi, das darin ursprünglich nur für einen der Ringe gestanden hatte.\n\nNach dem [[Zweiter Weltkrieg|Zweiten Weltkrieg]] wurde 1949 die ''Auto Union GmbH'' nun mit Sitz in [[Ingolstadt]] neugegründet. Nachdem diese sich zunächst auf die Marke ''DKW'' konzentriert hatte, wurde 1965 erstmals wieder die Marke ''Audi'' verwendet. Im Zuge der Fusion 1969 mit der ''[[NSU Motorenwerke|NSU Motorenwerke AG]]'' zur ''Audi NSU Auto Union AG'' wurde die Marke ''Audi'' zum ersten Mal nach 37 Jahren als prägender Bestandteil in den Firmennamen der Auto Union aufgenommen. Hauptsitz war, dem Fusionspartner entsprechend, bis 1985 in [[Neckarsulm]], bevor der Unternehmensname der ehemaligen Auto Union infolge des Auslaufens der Marke NSU auf ''Audi AG'' verkürzt wurde und der Sitz wieder zurück nach Ingolstadt wechselte.""")),
			WikipediaEntry("Electronic Arts", Option("""{{Infobox Unternehmen\n| Name             = Electronic Arts, Inc.\n| Logo             = [[Datei:Electronic-Arts-Logo.svg|200px]]\n| Unternehmensform = [[Gesellschaftsrecht der Vereinigten Staaten#Corporation|Corporation]]\n| ISIN             = US2855121099\n| Gründungsdatum   = 1982\n| Sitz             = [[Redwood City]], [[Vereinigte Staaten|USA]]\n| Leitung          = Andrew Wilson (CEO)<br />[[Larry Probst]] (Chairman)\n| Mitarbeiterzahl  = 9.300 (2013)<ref>Electronic Arts: [http://www.ea.com/about About EA]. Offizielle Unternehmenswebseite, zuletzt abgerufen am 31. Dezember 2013</ref>\n| Umsatz           = 3,575 Milliarden [[US-Dollar|USD]] <small>([[Geschäftsjahr|Fiskaljahr]] 2014)</small>\n| Branche          = [[Softwareentwicklung]]\n| Homepage         = [http://www.ea.com/de/ www.ea.com/de]\n}}\n'''Electronic Arts''' ('''EA''') ist ein börsennotierter, weltweit operierender Hersteller und [[Publisher]] von [[Computerspiel|Computer- und Videospielen]]. Das Unternehmen wurde vor allem für seine Sportspiele (''[[Madden NFL]]'', ''[[FIFA (Spieleserie)|FIFA]]'') bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von [[Vivendi Games]] und [[Activision]] zu [[Activision Blizzard]], war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt.<ref name="economist">''Looking forward to the next level. The world’s biggest games publisher sees good times ahead.'' In: ''The Economist.'' 8. Februar 2007, S.&nbsp;66.</ref> Die Aktien des Unternehmens sind im [[Nasdaq Composite]] und im [[S&P 500]] gelistet.""")),
			WikipediaEntry("Postbank-Hochhaus (Berlin)", Option("""{{Infobox Hohes Gebäude\n|Name=Postbank-Hochhaus Berlin\n|Bild=Postbank-Hochhaus Berlin.jpg|miniatur|Das Postbank-Hochhaus.\n|Ort=[[Berlin-Kreuzberg]]\n|Nutzung=Bürogebäude\n|Arbeitsplätze=\n|von=1965\n|bis=1971\n|Architekt=Prosper Lemoine\n|Baustil=[[Moderne (Architektur)|Moderne]]\n|Offizielle Höhe=89\n|Etagen=23\n|Fläche=\n|Geschossfläche=\n|Baustoffe=[[Stahlbeton]], [[Stahl]], Fassade aus [[Glas]]\n|Rang_Stadt=13\n|Rang_Land=\n|Stadt=Berlin\n|Land=Deutschland\n|Kontinent=Europa\n}}\n\nDas heutige '''Postbank-Hochhaus''' (früher: '''Postscheckamt Berlin West''' (Bln W), seit 1985: '''Postgiroamt Berlin''') ist ein [[Hochhaus]] der [[Postbank]] am [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Hallesches Ufer*|Halleschen Ufer]] 40–60 und der [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Großbeerenstraße*|Großbeerenstraße]] 2 im [[Berlin]]er Ortsteil [[Berlin-Kreuzberg|Kreuzberg]].\n\n== Geschichte und Entstehung ==\nDas Postscheckamt von Berlin war ab 1909 in einem Neubau in der [[Dorotheenstraße (Berlin)|Dorotheenstraße]] 29 (heute: 84), der einen Teil der ehemaligen [[Markthalle IV]] integrierte, untergebracht und war bis zum Ende des [[Zweiter Weltkrieg|Zweiten Weltkriegs]] für den Bereich der Städte Berlin, [[Frankfurt (Oder)]], [[Potsdam]], [[Magdeburg]] und [[Stettin]] zuständig. Aufgrund der [[Deutsche Teilung|Deutschen Teilung]] wurde das Postscheckamt in der Dorotheenstraße nur noch von der [[Deutsche Post (DDR)|Deutschen Post der DDR]] genutzt. Für den [[West-Berlin|Westteil von Berlin]] gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das ''Postscheckamt West'' eröffnet. 2014 kaufte die ''CG-Gruppe'' das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden.<ref>[http://www.berliner-zeitung.de/berlin/kreuzberg-wohnen-im-postbank-tower-3269104 ''Kreuzberg: Wohnen im Postbank-Tower''.] In: ''[[Berliner Zeitung]]'', 7. Februar 2014</ref>\n\n== Architektur ==\n[[Datei:Gottfried Gruner - Springbrunnen.jpg|mini|Springbrunnenanlage von [[Gottfried Gruner]]]]\n\nNach den Plänen des Oberpostdirektors [[Prosper Lemoine]] wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23&nbsp;[[Geschoss (Architektur)|Geschosse]] und gehört mit einer Höhe von 89&nbsp;Metern bis heute zu den [[Liste der Hochhäuser in Berlin|höchsten Gebäuden in Berlin]]. Das Hochhaus besitzt eine [[Aluminium]]-Glas-Fassade und wurde im sogenannten „[[Internationaler Stil|Internationalen Stil]]“ errichtet. Die Gestaltung des Gebäudes orientiert sich an [[Mies van der Rohe]]s [[Seagram Building]] in [[New York City|New York]].\n\nZu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der ''Große Brunnen'' von [[Gottfried Gruner]]. Er besteht aus 18&nbsp;Säulen aus [[Bronze]] und wurde 1972 in Betrieb genommen.\n\n== UKW-Sender ==\nIm Postbank-Hochhaus befinden sich mehrere [[Ultrakurzwellensender|UKW-Sender]], die von [[Media Broadcast]] betrieben werden. Die [[Deutsche Funkturm]] (DFMG), eine Tochtergesellschaft der [[Deutsche Telekom|Deutschen Telekom AG]], stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u.&nbsp;a. folgende Hörfunkprogramme auf [[Ultrakurzwelle]] ausgestrahlt:\n* [[88vier]], 500-W-Sender auf 88,4 MHz\n* [[NPR Berlin]], 400-W-Sender auf 104,1 MHz\n* [[Radio Russkij Berlin]], 100-W-Sender auf 97,2 MHz\n\n== Siehe auch ==\n* [[Postscheckamt]]\n* [[Postgeschichte und Briefmarken Berlins]]\n* [[Liste von Sendeanlagen in Berlin]]\n\n== Literatur ==\n* ''Vom Amt aus gesehen – Postscheckamt Berlin West (Prosper Lemoine).'' In: ''[[Bauwelt (Zeitschrift)|Bauwelt]].'' 43/1971 (Thema: Verwaltungsgebäude).\n\n== Weblinks ==\n{{Commons|Postbank (Berlin)}}\n* [http://www.luise-berlin.de/lexikon/frkr/p/postgiroamt.htm Postscheckamt Berlin West auf der Seite des Luisenstädter Bildungsvereins]\n* [http://www.bildhauerei-in-berlin.de/_html/_katalog/details-1445.html Bildhauerei in Berlin: Vorstellung des großen Brunnens]\n\n== Einzelnachweise ==\n<references />\n\n{{Coordinate|article=/|NS=52.499626|EW=13.383655|type=landmark|region=DE-BE}}\n\n{{SORTIERUNG:Postbank Hochhaus Berlin}}\n[[Kategorie:Berlin-Kreuzberg]]\n[[Kategorie:Erbaut in den 1970er Jahren]]\n[[Kategorie:Bürogebäude in Berlin]]\n[[Kategorie:Bauwerk der Moderne in Berlin]]\n[[Kategorie:Berlin-Mitte]]\n[[Kategorie:Hochhaus in Berlin]]\n[[Kategorie:Postgeschichte (Deutschland)]]\n[[Kategorie:Landwehrkanal]]\n[[Kategorie:Hochhaus in Europa]]""")),
			WikipediaEntry("Postbank-Hochhaus Berlin", Option("""#WEITERLEITUNG [[Postbank-Hochhaus (Berlin)]]""")),
			WikipediaEntry("Abraham Lincoln", Option("""{{Infobox officeholder\n| vicepresident = [[Hannibal Hamlin]]\n}}\nAbraham Lincoln was the 16th [[President of the United States]].\n{{Infobox U.S. Cabinet\n| Vice President 2 = [[Andrew Johnson]]\n}}\n{{multiple image\n | direction=horizontal\n | width=\n | footer=\n | width1=180\n | image1=A&TLincoln.jpg\n | alt1=A seated Lincoln holding a book as his young son looks at it\n | caption1=1864 photo of President Lincoln with youngest son, [[Tad Lincoln|Tad]]\n | width2=164\n | image2=Mary Todd Lincoln 1846-1847 restored cropped.png\n | alt2=Black and white photo of Mary Todd Lincoln's shoulders and head\n | caption2=[[Mary Todd Lincoln]], wife of Abraham Lincoln, age 28\n }}"""))
		))
	}

	def wikipediaTestAbstracts(): Map[String, String] = {
		Map(
			"Audi" -> """Die Audi AG (, Eigenschreibweise: AUDI AG) mit Sitz in Ingolstadt in Bayern ist ein deutscher Automobilhersteller, der dem Volkswagen-Konzern angehört. Der Markenname ist ein Wortspiel zur Umgehung der Namensrechte des ehemaligen Kraftfahrzeugherstellers A. Horch & Cie. Motorwagenwerke Zwickau.""",
			"Electronic Arts" -> """Electronic Arts (EA) ist ein börsennotierter, weltweit operierender Hersteller und Publisher von Computer- und Videospielen. Das Unternehmen wurde vor allem für seine Sportspiele (Madden NFL, FIFA) bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von Vivendi Games und Activision zu Activision Blizzard, war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt. Die Aktien des Unternehmens sind im Nasdaq Composite und im S&P 500 gelistet.""")
	}

	def groupedAliasesTestRDD(sc: SparkContext): RDD[Alias] = {
		sc.parallelize(List(
			Alias("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Alias("Bayern", Map("Bayern" -> 1).toSeq),
			Alias("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Alias("Zerfall", Map("Zerfall (Album)" -> 1, "Zerfall (Soziologie)" -> 1).toSeq)
		))
	}

	def groupedPagesTestRDD(sc: SparkContext): RDD[Page] = {
		sc.parallelize(List(
			Page("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Page("Bayern", Map("Bayern" -> 1).toSeq),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Page("Zerfall (Album)", Map("Zerfall" -> 1).toSeq),
			Page("Zerfall (Soziologie)", Map("Zerfall" -> 1).toSeq)
		))
	}

	def cleanedGroupedAliasesTestRDD(sc: SparkContext): RDD[Alias] = {
		sc.parallelize(List(
			Alias("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Alias("Bayern", Map("Bayern" -> 1).toSeq),
			Alias("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Alias("Zerfall", Map("Zerfall (Album)" -> 1).toSeq)
		))
	}

	def cleanedGroupedPagesTestRDD(sc: SparkContext): RDD[Page] = {
		sc.parallelize(List(
			Page("Ingolstadt", Map("Ingolstadt" -> 1).toSeq),
			Page("Bayern", Map("Bayern" -> 1).toSeq),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1).toSeq),
			Page("Zerfall (Album)", Map("Zerfall" -> 1).toSeq)
		))
	}

	def probabilityReferences(): Map[String, Double] = {
		Map(
			"Ingolstadt" -> 0.0,
			"Bayern" -> 1.0,
			"Automobilhersteller" -> 0.0,
			"Zerfall" -> 0.0
		)
	}

	def allPagesTestRDD(sc: SparkContext): RDD[String] = {
		sc.parallelize(List(
			"Automobilhersteller",
			"Ingolstadt",
			"Bayern",
			"Zerfall (Album)"
		))
	}

	def smallerParsedWikipediaTestRDD(sc: SparkContext): RDD[ParsedWikipediaEntry] = {
		sc.parallelize(List(
			ParsedWikipediaEntry("Audi", Option("dummy text"), List(
				Link("Ingolstadt", "Ingolstadt", 55),
				Link("Bayern", "Bayern", 69),
				Link("Automobilhersteller", "Automobilhersteller", 94),
				Link("Zerfall", "Zerfall (Album)", 4711),
				Link("Zerfall", "Zerfall (Soziologie)", 4711) // dead link
			))))
	}

	def germanStopwordsTestSet(): Set[String] = {
		Set("der", "die", "das", "und", "als", "ist", "an", "am", "im", "dem", "des")
	}

	// scalastyle:on line.size.limit
}
