import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

class WikipediaTextparserTest extends FlatSpec with SharedSparkContext {
	"Wikipedia entry title" should "not change" in {
		wikipediaTestRDD()
			.map(entry => (entry.title, (entry, WikipediaTextparser.wikipediaToHtml(entry.getText))))
			.map(tuple => (tuple._1, WikipediaTextparser.parseHtml(tuple._2).title))
			.collect
			.foreach(entry => assert(entry._1 == entry._2))
	}

	"Wikipedia article text" should "not contain Wikimarkup" in {
		// matches [[...]] and {{...}} but not escaped '{', i.e. "\{"
		val wikimarkupRegex = new Regex("(\\[\\[.*?\\]\\])" + "|" + "([^\\\\]\\{\\{.*?\\}\\})")
		wikipediaTestRDD()
			.map(entry => WikipediaTextparser.wikipediaToHtml(entry.getText))
			.collect
			.foreach(element => assert(wikimarkupRegex.findFirstIn(element).isEmpty))
	}

	"Wikipedia article text" should "not contain escaped HTML characters" in {
		val wikimarkupRegex = new Regex("&\\S*?;")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText)))
			.map(WikipediaTextparser.parseHtml)
			.collect
			.foreach(element => assert(wikimarkupRegex.findFirstIn(element.getText).isEmpty))
	}

	"Wikipedia article text" should "contain none of the tags: table, h[0-9], small" in {
		val tagBlacklist = List[String]("table", "h[0-9]", "small")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText)))
			.map(WikipediaTextparser.parseHtml)
			.map(_.getText)
			.collect
			.foreach { element =>
				for (tag <- tagBlacklist) {
					val tagRegex = new Regex("(</" + tag + ">)|(<" + tag + "(>| .*?>))")
					assert(tagRegex.findFirstIn(element).isEmpty)
				}
			}
	}

	"Wikipedia article plain text" should "be complete" in {
		val abstracts = Map(
			"Audi" -> """Die Audi AG (, Eigenschreibweise: AUDI AG) mit Sitz in Ingolstadt in Bayern ist ein deutscher Automobilhersteller, der dem Volkswagen-Konzern angehört. Der Markenname ist ein Wortspiel zur Umgehung der Namensrechte des ehemaligen Kraftfahrzeugherstellers A. Horch & Cie. Motorwagenwerke Zwickau.""",
			"Electronic Arts" -> """Electronic Arts (EA) ist ein börsennotierter, weltweit operierender Hersteller und Publisher von Computer- und Videospielen. Das Unternehmen wurde vor allem für seine Sportspiele (Madden NFL, FIFA) bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von Vivendi Games und Activision zu Activision Blizzard, war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt. Die Aktien des Unternehmens sind im Nasdaq Composite und im S&P 500 gelistet.""")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText)))
			.map(WikipediaTextparser.parseHtml)
			.filter(entry => abstracts.contains(entry.title))
			.collect
			.foreach(element => assert(element.getText.startsWith(abstracts(element.title))))
	}

	"Wikipedia links" should "not be empty" in {
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText)))
			.map(WikipediaTextparser.parseHtml)
			.map(_.links)
			.collect
			.foreach(element => assert(element.nonEmpty))
	}

	"Wikipedia links" should "contain links from infobox if in article" in {
		val infoboxArticles = wikipediaTestInfoboxArticles()
		wikipediaTestRDD()
			.filter(entry => infoboxArticles.contains(entry.title))
			.map(WikipediaTextparser.parseWikipediaEntry)
			.map(_.links)
			.collect
			.foreach(links => assert(links.exists(link => link.offset == WikipediaTextparser.infoboxOffset)))
	}

	def isTextLinkConsistent(link: WikipediaTextparser.Link, text: String): Boolean = {
		val substring = text.substring(link.offset, link.offset + link.alias.length)
		substring == link.alias
	}

	def isInfoboxLink(link: WikipediaTextparser.Link): Boolean = {
		link.offset == WikipediaTextparser.infoboxOffset
	}

	def isLinkValid(link: WikipediaTextparser.Link): Boolean = {
		val result = link.alias.nonEmpty && link.page.nonEmpty
		if(!result)
			println(link)
		result
	}

	"Wikipedia links" should "be valid (have alias and page)" in {
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText)))
			.map(WikipediaTextparser.parseHtml)
			.flatMap(_.links)
			.collect
			.foreach(link => assert(isLinkValid(link)))
	}

	"Wikipedia text link offsets" should "be consistent with text" in {
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText)))
			.map(WikipediaTextparser.parseHtml)
			.collect
			.foreach { element =>
				element.links.foreach { link =>
					if (!isInfoboxLink(link)) {
						assert(isTextLinkConsistent(link, element.getText))
					}
				}
			}
	}

	"Wikipedia links" should "be exactly these links" in {
		val links = wikipediaTestReferences()
		wikipediaTestRDD()
			.map(WikipediaTextparser.parseWikipediaEntry)
			.filter(entry => links.contains(entry.title))
			.collect
			.foreach{entry =>
				for (i <- entry.links.indices)
					assert(entry.links(i) == links(entry.title)(i)) // elementwise comparision shows where it fails
			}
	}

	// extracted links from Article abstracts
	def wikipediaTestReferences(): Map[String, List[WikipediaTextparser.Link]] = {
		Map(
			"Audi" -> List(
				// Article text links
				WikipediaTextparser.Link("Ingolstadt", "Ingolstadt", 55),
				WikipediaTextparser.Link("Bayern", "Bayern", 69),
				WikipediaTextparser.Link("Automobilhersteller", "Automobilhersteller", 94),
				WikipediaTextparser.Link("Volkswagen", "Volkswagen AG", 123),
				WikipediaTextparser.Link("Wortspiel", "Wortspiel", 175),
				WikipediaTextparser.Link("Namensrechte", "Marke (Recht)", 202),
				WikipediaTextparser.Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", 255),
				WikipediaTextparser.Link("August Horch", "August Horch", 316),
				WikipediaTextparser.Link("Lateinische", "Latein", 599),
				WikipediaTextparser.Link("Imperativ", "Imperativ (Modus)", 636),
				WikipediaTextparser.Link("Zwickau", "Zwickau", 829),
				WikipediaTextparser.Link("Zschopauer", "Zschopau", 868),
				WikipediaTextparser.Link("DKW", "DKW", 937),
				WikipediaTextparser.Link("Wanderer", "Wanderer (Unternehmen)", 1071),
				WikipediaTextparser.Link("Auto Union AG", "Auto Union", 1105),
				WikipediaTextparser.Link("Chemnitz", "Chemnitz", 1131),
				WikipediaTextparser.Link("Zweiten Weltkrieg", "Zweiter Weltkrieg", 1358),
				WikipediaTextparser.Link("Ingolstadt", "Ingolstadt", 1423),
				WikipediaTextparser.Link("NSU Motorenwerke AG", "NSU Motorenwerke", 1599),
				WikipediaTextparser.Link("Neckarsulm", "Neckarsulm", 1830),

				// Infobox links
				WikipediaTextparser.Link("Aktiengesellschaft", "Aktiengesellschaft", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Zwickau", "Zwickau", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Chemnitz", "Chemnitz", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Ingolstadt", "Ingolstadt", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Neckarsulm", "Neckarsulm", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Ingolstadt", "Ingolstadt", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Deutschland", "Deutschland", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Rupert Stadler", "Rupert Stadler", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Vorstand", "Vorstand", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Matthias Müller", "Matthias Müller (Manager)", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Aufsichtsrat", "Aufsichtsrat", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Mrd.", "Milliarde", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("EUR", "Euro", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Automobilhersteller", "Automobilhersteller", WikipediaTextparser.infoboxOffset)),

			"Electronic Arts" -> List(
				// Article text links
				WikipediaTextparser.Link("Publisher", "Publisher", 83),
				WikipediaTextparser.Link("Computer- und Videospielen", "Computerspiel", 97),
				WikipediaTextparser.Link("Madden NFL", "Madden NFL", 180),
				WikipediaTextparser.Link("FIFA", "FIFA (Spieleserie)", 192),
				WikipediaTextparser.Link("Vivendi Games", "Vivendi Games", 346),
				WikipediaTextparser.Link("Activision", "Activision", 364),
				WikipediaTextparser.Link("Activision Blizzard", "Activision Blizzard", 378),
				WikipediaTextparser.Link("Nasdaq Composite", "Nasdaq Composite", 675),
				WikipediaTextparser.Link("S&P 500", "S&P 500", 699),

				// Infobox links
				WikipediaTextparser.Link("Corporation", "Gesellschaftsrecht der Vereinigten Staaten#Corporation", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Redwood City", "Redwood City", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("USA", "Vereinigte Staaten", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Larry Probst", "Larry Probst", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("USD", "US-Dollar", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Fiskaljahr", "Geschäftsjahr", WikipediaTextparser.infoboxOffset),
				WikipediaTextparser.Link("Softwareentwicklung", "Softwareentwicklung", WikipediaTextparser.infoboxOffset)))
	}

	def wikipediaTestInfoboxArticles(): Set[String] = {
		Set("Audi", "Electronic Arts", "Postbank-Hochhaus (Berlin)")
	}

	// extracted from Wikipedia
	def wikipediaTestRDD(): RDD[WikipediaTextparser.WikipediaEntry] = {
		sc.parallelize(List(
			new WikipediaTextparser.WikipediaEntry("Audi", """{{Begriffsklärungshinweis}}\n{{Coordinate |NS=48/46/59.9808/N |EW=11/25/4.926/E |type=landmark |region=DE-BY }}\n{{Infobox Unternehmen\n| Name   = Audi AG\n| Logo   = Audi-Logo 2016.svg\n| Unternehmensform = [[Aktiengesellschaft]]\n| Gründungsdatum = 16. Juli 1909 in [[Zwickau]] (Audi)<br /><!--\n-->29. Juni 1932 in [[Chemnitz]] (Auto Union)<br /><!--\n-->3.&nbsp;September&nbsp;1949&nbsp;in&nbsp;[[Ingolstadt]]&nbsp;(Neugründung)<br /><!--\n-->10. März 1969 in [[Neckarsulm]] (Fusion)\n| ISIN   = DE0006757008\n| Sitz   = [[Ingolstadt]], [[Deutschland]]\n| Leitung  =\n* [[Rupert Stadler]],<br />[[Vorstand]]svorsitzender\n* [[Matthias Müller (Manager)|Matthias Müller]],<br />[[Aufsichtsrat]]svorsitzender\n| Mitarbeiterzahl = 82.838 <small>(31. Dez. 2015)</small><ref name="kennzahlen" />\n| Umsatz  = 58,42 [[Milliarde|Mrd.]] [[Euro|EUR]] <small>(2015)</small><ref name="kennzahlen" />\n| Branche  = [[Automobilhersteller]]\n| Homepage  = www.audi.de\n}}\n\n[[Datei:Audi Ingolstadt.jpg|mini|Hauptsitz in Ingolstadt]]\n[[Datei:Neckarsulm 20070725.jpg|mini|Audi-Werk in Neckarsulm (Bildmitte)]]\n[[Datei:Michèle Mouton, Audi Quattro A1 - 1983 (11).jpg|mini|Kühlergrill mit Audi-Emblem <small>[[Audi quattro]] (Rallye-Ausführung, Baujahr 1983)</small>]]\n[[Datei:Audi 2009 logo.svg|mini|Logo bis April 2016]]\n\nDie '''Audi AG''' ({{Audio|Audi AG.ogg|Aussprache}}, Eigenschreibweise: ''AUDI AG'') mit Sitz in [[Ingolstadt]] in [[Bayern]] ist ein deutscher [[Automobilhersteller]], der dem [[Volkswagen AG|Volkswagen]]-Konzern angehört.\n\nDer Markenname ist ein [[Wortspiel]] zur Umgehung der [[Marke (Recht)|Namensrechte]] des ehemaligen Kraftfahrzeugherstellers ''[[Horch|A. Horch & Cie. Motorwagenwerke Zwickau]]''. Unternehmensgründer [[August Horch]], der „seine“ Firma nach Zerwürfnissen mit dem Finanzvorstand verlassen hatte, suchte einen Namen für sein neues Unternehmen und fand ihn im Vorschlag des Zwickauer Gymnasiasten Heinrich Finkentscher (Sohn des mit A. Horch befreundeten Franz Finkentscher), der ''Horch'' ins [[Latein]]ische übersetzte.<ref>Film der Audi AG: ''Die Silberpfeile aus Zwickau.'' Interview mit August Horch, Video 1992.</ref> ''Audi'' ist der [[Imperativ (Modus)|Imperativ]] Singular von ''audire'' (zu Deutsch ''hören'', ''zuhören'') und bedeutet „Höre!“ oder eben „Horch!“. Am 25. April 1910 wurde die ''Audi Automobilwerke GmbH Zwickau'' in das Handelsregister der Stadt [[Zwickau]] eingetragen.\n\n1928 übernahm die [[Zschopau]]er ''Motorenwerke J. S. Rasmussen AG'', bekannt durch ihre Marke ''[[DKW]]'', die Audi GmbH. Audi wurde zur Tochtergesellschaft und 1932 mit der Übernahme der Horchwerke AG sowie einem Werk des Unternehmens ''[[Wanderer (Unternehmen)|Wanderer]]'' Teil der neu gegründeten ''[[Auto Union|Auto Union AG]]'' mit Sitz in [[Chemnitz]], die folglich die vier verschiedenen Marken unter einem Dach anboten. Daraus entstand auch das heutige aus vier Ringen bestehende Logo von Audi, das darin ursprünglich nur für einen der Ringe gestanden hatte.\n\nNach dem [[Zweiter Weltkrieg|Zweiten Weltkrieg]] wurde 1949 die ''Auto Union GmbH'' nun mit Sitz in [[Ingolstadt]] neugegründet. Nachdem diese sich zunächst auf die Marke ''DKW'' konzentriert hatte, wurde 1965 erstmals wieder die Marke ''Audi'' verwendet. Im Zuge der Fusion 1969 mit der ''[[NSU Motorenwerke|NSU Motorenwerke AG]]'' zur ''Audi NSU Auto Union AG'' wurde die Marke ''Audi'' zum ersten Mal nach 37 Jahren als prägender Bestandteil in den Firmennamen der Auto Union aufgenommen. Hauptsitz war, dem Fusionspartner entsprechend, bis 1985 in [[Neckarsulm]], bevor der Unternehmensname der ehemaligen Auto Union infolge des Auslaufens der Marke NSU auf ''Audi AG'' verkürzt wurde und der Sitz wieder zurück nach Ingolstadt wechselte.""", null),
			new WikipediaTextparser.WikipediaEntry("Electronic Arts", """{{Infobox Unternehmen\n| Name             = Electronic Arts, Inc.\n| Logo             = [[Datei:Electronic-Arts-Logo.svg|200px]]\n| Unternehmensform = [[Gesellschaftsrecht der Vereinigten Staaten#Corporation|Corporation]]\n| ISIN             = US2855121099\n| Gründungsdatum   = 1982\n| Sitz             = [[Redwood City]], [[Vereinigte Staaten|USA]]\n| Leitung          = Andrew Wilson (CEO)<br />[[Larry Probst]] (Chairman)\n| Mitarbeiterzahl  = 9.300 (2013)<ref>Electronic Arts: [http://www.ea.com/about About EA]. Offizielle Unternehmenswebseite, zuletzt abgerufen am 31. Dezember 2013</ref>\n| Umsatz           = 3,575 Milliarden [[US-Dollar|USD]] <small>([[Geschäftsjahr|Fiskaljahr]] 2014)</small>\n| Branche          = [[Softwareentwicklung]]\n| Homepage         = [http://www.ea.com/de/ www.ea.com/de]\n}}\n'''Electronic Arts''' ('''EA''') ist ein börsennotierter, weltweit operierender Hersteller und [[Publisher]] von [[Computerspiel|Computer- und Videospielen]]. Das Unternehmen wurde vor allem für seine Sportspiele (''[[Madden NFL]]'', ''[[FIFA (Spieleserie)|FIFA]]'') bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von [[Vivendi Games]] und [[Activision]] zu [[Activision Blizzard]], war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt.<ref name="economist">''Looking forward to the next level. The world’s biggest games publisher sees good times ahead.'' In: ''The Economist.'' 8. Februar 2007, S.&nbsp;66.</ref> Die Aktien des Unternehmens sind im [[Nasdaq Composite]] und im [[S&P 500]] gelistet.""", null),
			new WikipediaTextparser.WikipediaEntry("Postbank-Hochhaus (Berlin)", """{{Infobox Hohes Gebäude\n|Name=Postbank-Hochhaus Berlin\n|Bild=Postbank-Hochhaus Berlin.jpg|miniatur|Das Postbank-Hochhaus.\n|Ort=[[Berlin-Kreuzberg]]\n|Nutzung=Bürogebäude\n|Arbeitsplätze=\n|von=1965\n|bis=1971\n|Architekt=Prosper Lemoine\n|Baustil=[[Moderne (Architektur)|Moderne]]\n|Offizielle Höhe=89\n|Etagen=23\n|Fläche=\n|Geschossfläche=\n|Baustoffe=[[Stahlbeton]], [[Stahl]], Fassade aus [[Glas]]\n|Rang_Stadt=13\n|Rang_Land=\n|Stadt=Berlin\n|Land=Deutschland\n|Kontinent=Europa\n}}\n\nDas heutige '''Postbank-Hochhaus''' (früher: '''Postscheckamt Berlin West''' (Bln W), seit 1985: '''Postgiroamt Berlin''') ist ein [[Hochhaus]] der [[Postbank]] am [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Hallesches Ufer*|Halleschen Ufer]] 40–60 und der [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Großbeerenstraße*|Großbeerenstraße]] 2 im [[Berlin]]er Ortsteil [[Berlin-Kreuzberg|Kreuzberg]].\n\n== Geschichte und Entstehung ==\nDas Postscheckamt von Berlin war ab 1909 in einem Neubau in der [[Dorotheenstraße (Berlin)|Dorotheenstraße]] 29 (heute: 84), der einen Teil der ehemaligen [[Markthalle IV]] integrierte, untergebracht und war bis zum Ende des [[Zweiter Weltkrieg|Zweiten Weltkriegs]] für den Bereich der Städte Berlin, [[Frankfurt (Oder)]], [[Potsdam]], [[Magdeburg]] und [[Stettin]] zuständig. Aufgrund der [[Deutsche Teilung|Deutschen Teilung]] wurde das Postscheckamt in der Dorotheenstraße nur noch von der [[Deutsche Post (DDR)|Deutschen Post der DDR]] genutzt. Für den [[West-Berlin|Westteil von Berlin]] gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das ''Postscheckamt West'' eröffnet. 2014 kaufte die ''CG-Gruppe'' das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden.<ref>[http://www.berliner-zeitung.de/berlin/kreuzberg-wohnen-im-postbank-tower-3269104 ''Kreuzberg: Wohnen im Postbank-Tower''.] In: ''[[Berliner Zeitung]]'', 7. Februar 2014</ref>\n\n== Architektur ==\n[[Datei:Gottfried Gruner - Springbrunnen.jpg|mini|Springbrunnenanlage von [[Gottfried Gruner]]]]\n\nNach den Plänen des Oberpostdirektors [[Prosper Lemoine]] wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23&nbsp;[[Geschoss (Architektur)|Geschosse]] und gehört mit einer Höhe von 89&nbsp;Metern bis heute zu den [[Liste der Hochhäuser in Berlin|höchsten Gebäuden in Berlin]]. Das Hochhaus besitzt eine [[Aluminium]]-Glas-Fassade und wurde im sogenannten „[[Internationaler Stil|Internationalen Stil]]“ errichtet. Die Gestaltung des Gebäudes orientiert sich an [[Mies van der Rohe]]s [[Seagram Building]] in [[New York City|New York]].\n\nZu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der ''Große Brunnen'' von [[Gottfried Gruner]]. Er besteht aus 18&nbsp;Säulen aus [[Bronze]] und wurde 1972 in Betrieb genommen.\n\n== UKW-Sender ==\nIm Postbank-Hochhaus befinden sich mehrere [[Ultrakurzwellensender|UKW-Sender]], die von [[Media Broadcast]] betrieben werden. Die [[Deutsche Funkturm]] (DFMG), eine Tochtergesellschaft der [[Deutsche Telekom|Deutschen Telekom AG]], stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u.&nbsp;a. folgende Hörfunkprogramme auf [[Ultrakurzwelle]] ausgestrahlt:\n* [[88vier]], 500-W-Sender auf 88,4 MHz\n* [[NPR Berlin]], 400-W-Sender auf 104,1 MHz\n* [[Radio Russkij Berlin]], 100-W-Sender auf 97,2 MHz\n\n== Siehe auch ==\n* [[Postscheckamt]]\n* [[Postgeschichte und Briefmarken Berlins]]\n* [[Liste von Sendeanlagen in Berlin]]\n\n== Literatur ==\n* ''Vom Amt aus gesehen – Postscheckamt Berlin West (Prosper Lemoine).'' In: ''[[Bauwelt (Zeitschrift)|Bauwelt]].'' 43/1971 (Thema: Verwaltungsgebäude).\n\n== Weblinks ==\n{{Commons|Postbank (Berlin)}}\n* [http://www.luise-berlin.de/lexikon/frkr/p/postgiroamt.htm Postscheckamt Berlin West auf der Seite des Luisenstädter Bildungsvereins]\n* [http://www.bildhauerei-in-berlin.de/_html/_katalog/details-1445.html Bildhauerei in Berlin: Vorstellung des großen Brunnens]\n\n== Einzelnachweise ==\n<references />\n\n{{Coordinate|article=/|NS=52.499626|EW=13.383655|type=landmark|region=DE-BE}}\n\n{{SORTIERUNG:Postbank Hochhaus Berlin}}\n[[Kategorie:Berlin-Kreuzberg]]\n[[Kategorie:Erbaut in den 1970er Jahren]]\n[[Kategorie:Bürogebäude in Berlin]]\n[[Kategorie:Bauwerk der Moderne in Berlin]]\n[[Kategorie:Berlin-Mitte]]\n[[Kategorie:Hochhaus in Berlin]]\n[[Kategorie:Postgeschichte (Deutschland)]]\n[[Kategorie:Landwehrkanal]]\n[[Kategorie:Hochhaus in Europa]]""", null),
			new WikipediaTextparser.WikipediaEntry("Postbank-Hochhaus Berlin", """#WEITERLEITUNG [[Postbank-Hochhaus (Berlin)]]""", null)
		))
		// sc.getTextFile("testwiki.txt").map(data => new WikipediaTextparser.WikipediaEntry("Audi", data, null))
	}
}
