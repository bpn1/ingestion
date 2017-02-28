import WikiClasses._
import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex
import org.scalatest._

class WikipediaTextparserTest extends FlatSpec with SharedSparkContext with Matchers {
	"Wikipedia entry title" should "not change" in {
		wikipediaTestRDD()
			.map(entry => (entry.title, (entry, WikipediaTextparser.wikipediaToHtml(entry.getText()))))
			.map(tuple => (tuple._1, WikipediaTextparser.parseHtml(tuple._2).title))
			.collect
			.foreach(entry => entry._1 shouldEqual entry._2)
	}

	"Wikipedia article text" should "not contain Wikimarkup" in {
		// matches [[...]] and {{...}} but not escaped '{', i.e. "\{"
		val wikimarkupRegex = new Regex("(\\[\\[.*?\\]\\])" + "|" + "([^\\\\]\\{\\{.*?\\}\\})")
		wikipediaTestRDD()
			.map(entry => WikipediaTextparser.wikipediaToHtml(entry.getText()))
			.collect
			.foreach(element => wikimarkupRegex.findFirstIn(element) shouldBe empty)
	}
	it should "not contain escaped HTML characters" in {
		val wikimarkupRegex = new Regex("&\\S*?;")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.collect
			.foreach(element => wikimarkupRegex.findFirstIn(element.getText()) shouldBe empty)
	}
	it should "contain none of the tags: table, h[0-9], small" in {
		val tagBlacklist = List[String]("table", "h[0-9]", "small")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.map(_.getText())
			.collect
			.foreach { element =>
				for (tag <- tagBlacklist) {
					val tagRegex = new Regex("(</" + tag + ">)|(<" + tag + "(>| .*?>))")
					tagRegex.findFirstIn(element) shouldBe empty
				}
			}
	}
	it should "be complete" in {
		val abstracts = Map(
			"Audi" -> """Die Audi AG (, Eigenschreibweise: AUDI AG) mit Sitz in Ingolstadt in Bayern ist ein deutscher Automobilhersteller, der dem Volkswagen-Konzern angehört. Der Markenname ist ein Wortspiel zur Umgehung der Namensrechte des ehemaligen Kraftfahrzeugherstellers A. Horch & Cie. Motorwagenwerke Zwickau.""",
			"Electronic Arts" -> """Electronic Arts (EA) ist ein börsennotierter, weltweit operierender Hersteller und Publisher von Computer- und Videospielen. Das Unternehmen wurde vor allem für seine Sportspiele (Madden NFL, FIFA) bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von Vivendi Games und Activision zu Activision Blizzard, war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt. Die Aktien des Unternehmens sind im Nasdaq Composite und im S&P 500 gelistet.""")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.filter(entry => abstracts.contains(entry.title))
			.collect
			.foreach(element => element.getText() should startWith(abstracts(element.title)))
	}

	"Wikipedia links" should "not be empty" in {
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.map(_.links)
			.collect
			.foreach(links => links should not be empty)
	}
	they should "contain links from templates in article" in {
		val templateArticlesTest = wikipediaTestTemplateArticles()
		wikipediaTestRDD()
			.filter(entry => templateArticlesTest.contains(entry.title))
			.map(WikipediaTextparser.parseWikipediaEntry)
			.map(_.links)
			.collect
			.foreach(links => assert(links.exists(link => link.offset == WikipediaTextparser.templateOffset)))
	}
	they should "be valid (have alias and page)" in {
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.flatMap(_.links)
			.collect
			.foreach(link => assert(isLinkValid(link)))
	}
	they should "be exactly these links" in {
		val links = wikipediaTestReferences()
		wikipediaTestRDD()
			.map(WikipediaTextparser.parseWikipediaEntry)
			.filter(entry => links.contains(entry.title))
			.collect
			.foreach(entry => entry.links shouldEqual links(entry.title))
	}

	"Wikipedia text link offsets" should "be consistent with text" in {
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.getText())))
			.map(WikipediaTextparser.parseHtml)
			.collect
			.foreach { element =>
				element.links.foreach { link =>
					if (!isTemplateLink(link)) {
						if (!isTextLinkConsistent(link, element.getText())) {
							println(link)
							println(element.getText())
						}

						assert(isTextLinkConsistent(link, element.getText()))
					}
				}
			}
	}

	def isTextLinkConsistent(link: Link, text: String): Boolean = {
		val substring = text.substring(link.offset, link.offset + link.alias.length)
		substring == link.alias
	}

	def isTemplateLink(link: Link): Boolean = {
		link.offset == WikipediaTextparser.templateOffset
	}

	def isLinkValid(link: Link): Boolean = {
		val result = link.alias.nonEmpty && link.page.nonEmpty
		if (!result)
			println(link)
		result
	}

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

	def wikipediaTestTemplateArticles(): Set[String] = {
		Set("Audi", "Electronic Arts", "Postbank-Hochhaus (Berlin)", "Abraham Lincoln")
	}

	def wikipediaTestRDD(): RDD[WikipediaEntry] = {
		// extracted from Wikipedia
		sc.parallelize(List(
			WikipediaEntry("Audi", Option("""{{Begriffsklärungshinweis}}\n{{Coordinate |NS=48/46/59.9808/N |EW=11/25/4.926/E |type=landmark |region=DE-BY }}\n{{Infobox Unternehmen\n| Name   = Audi AG\n| Logo   = Audi-Logo 2016.svg\n| Unternehmensform = [[Aktiengesellschaft]]\n| Gründungsdatum = 16. Juli 1909 in [[Zwickau]] (Audi)<br /><!--\n-->29. Juni 1932 in [[Chemnitz]] (Auto Union)<br /><!--\n-->3.&nbsp;September&nbsp;1949&nbsp;in&nbsp;[[Ingolstadt]]&nbsp;(Neugründung)<br /><!--\n-->10. März 1969 in [[Neckarsulm]] (Fusion)\n| ISIN   = DE0006757008\n| Sitz   = [[Ingolstadt]], [[Deutschland]]\n| Leitung  =\n* [[Rupert Stadler]],<br />[[Vorstand]]svorsitzender\n* [[Matthias Müller (Manager)|Matthias Müller]],<br />[[Aufsichtsrat]]svorsitzender\n| Mitarbeiterzahl = 82.838 <small>(31. Dez. 2015)</small><ref name="kennzahlen" />\n| Umsatz  = 58,42 [[Milliarde|Mrd.]] [[Euro|EUR]] <small>(2015)</small><ref name="kennzahlen" />\n| Branche  = [[Automobilhersteller]]\n| Homepage  = www.audi.de\n}}\n\n[[Datei:Audi Ingolstadt.jpg|mini|Hauptsitz in Ingolstadt]]\n[[Datei:Neckarsulm 20070725.jpg|mini|Audi-Werk in Neckarsulm (Bildmitte)]]\n[[Datei:Michèle Mouton, Audi Quattro A1 - 1983 (11).jpg|mini|Kühlergrill mit Audi-Emblem <small>[[Audi quattro]] (Rallye-Ausführung, Baujahr 1983)</small>]]\n[[Datei:Audi 2009 logo.svg|mini|Logo bis April 2016]]\n\nDie '''Audi AG''' ({{Audio|Audi AG.ogg|Aussprache}}, Eigenschreibweise: ''AUDI AG'') mit Sitz in [[Ingolstadt]] in [[Bayern]] ist ein deutscher [[Automobilhersteller]], der dem [[Volkswagen AG|Volkswagen]]-Konzern angehört.\n\nDer Markenname ist ein [[Wortspiel]] zur Umgehung der [[Marke (Recht)|Namensrechte]] des ehemaligen Kraftfahrzeugherstellers ''[[Horch|A. Horch & Cie. Motorwagenwerke Zwickau]]''. Unternehmensgründer [[August Horch]], der „seine“ Firma nach Zerwürfnissen mit dem Finanzvorstand verlassen hatte, suchte einen Namen für sein neues Unternehmen und fand ihn im Vorschlag des Zwickauer Gymnasiasten Heinrich Finkentscher (Sohn des mit A. Horch befreundeten Franz Finkentscher), der ''Horch'' ins [[Latein]]ische übersetzte.<ref>Film der Audi AG: ''Die Silberpfeile aus Zwickau.'' Interview mit August Horch, Video 1992.</ref> ''Audi'' ist der [[Imperativ (Modus)|Imperativ]] Singular von ''audire'' (zu Deutsch ''hören'', ''zuhören'') und bedeutet „Höre!“ oder eben „Horch!“. Am 25. April 1910 wurde die ''Audi Automobilwerke GmbH Zwickau'' in das Handelsregister der Stadt [[Zwickau]] eingetragen.\n\n1928 übernahm die [[Zschopau]]er ''Motorenwerke J. S. Rasmussen AG'', bekannt durch ihre Marke ''[[DKW]]'', die Audi GmbH. Audi wurde zur Tochtergesellschaft und 1932 mit der Übernahme der Horchwerke AG sowie einem Werk des Unternehmens ''[[Wanderer (Unternehmen)|Wanderer]]'' Teil der neu gegründeten ''[[Auto Union|Auto Union AG]]'' mit Sitz in [[Chemnitz]], die folglich die vier verschiedenen Marken unter einem Dach anboten. Daraus entstand auch das heutige aus vier Ringen bestehende Logo von Audi, das darin ursprünglich nur für einen der Ringe gestanden hatte.\n\nNach dem [[Zweiter Weltkrieg|Zweiten Weltkrieg]] wurde 1949 die ''Auto Union GmbH'' nun mit Sitz in [[Ingolstadt]] neugegründet. Nachdem diese sich zunächst auf die Marke ''DKW'' konzentriert hatte, wurde 1965 erstmals wieder die Marke ''Audi'' verwendet. Im Zuge der Fusion 1969 mit der ''[[NSU Motorenwerke|NSU Motorenwerke AG]]'' zur ''Audi NSU Auto Union AG'' wurde die Marke ''Audi'' zum ersten Mal nach 37 Jahren als prägender Bestandteil in den Firmennamen der Auto Union aufgenommen. Hauptsitz war, dem Fusionspartner entsprechend, bis 1985 in [[Neckarsulm]], bevor der Unternehmensname der ehemaligen Auto Union infolge des Auslaufens der Marke NSU auf ''Audi AG'' verkürzt wurde und der Sitz wieder zurück nach Ingolstadt wechselte.""")),
			WikipediaEntry("Electronic Arts", Option("""{{Infobox Unternehmen\n| Name             = Electronic Arts, Inc.\n| Logo             = [[Datei:Electronic-Arts-Logo.svg|200px]]\n| Unternehmensform = [[Gesellschaftsrecht der Vereinigten Staaten#Corporation|Corporation]]\n| ISIN             = US2855121099\n| Gründungsdatum   = 1982\n| Sitz             = [[Redwood City]], [[Vereinigte Staaten|USA]]\n| Leitung          = Andrew Wilson (CEO)<br />[[Larry Probst]] (Chairman)\n| Mitarbeiterzahl  = 9.300 (2013)<ref>Electronic Arts: [http://www.ea.com/about About EA]. Offizielle Unternehmenswebseite, zuletzt abgerufen am 31. Dezember 2013</ref>\n| Umsatz           = 3,575 Milliarden [[US-Dollar|USD]] <small>([[Geschäftsjahr|Fiskaljahr]] 2014)</small>\n| Branche          = [[Softwareentwicklung]]\n| Homepage         = [http://www.ea.com/de/ www.ea.com/de]\n}}\n'''Electronic Arts''' ('''EA''') ist ein börsennotierter, weltweit operierender Hersteller und [[Publisher]] von [[Computerspiel|Computer- und Videospielen]]. Das Unternehmen wurde vor allem für seine Sportspiele (''[[Madden NFL]]'', ''[[FIFA (Spieleserie)|FIFA]]'') bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von [[Vivendi Games]] und [[Activision]] zu [[Activision Blizzard]], war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt.<ref name="economist">''Looking forward to the next level. The world’s biggest games publisher sees good times ahead.'' In: ''The Economist.'' 8. Februar 2007, S.&nbsp;66.</ref> Die Aktien des Unternehmens sind im [[Nasdaq Composite]] und im [[S&P 500]] gelistet.""")),
			WikipediaEntry("Postbank-Hochhaus (Berlin)", Option("""{{Infobox Hohes Gebäude\n|Name=Postbank-Hochhaus Berlin\n|Bild=Postbank-Hochhaus Berlin.jpg|miniatur|Das Postbank-Hochhaus.\n|Ort=[[Berlin-Kreuzberg]]\n|Nutzung=Bürogebäude\n|Arbeitsplätze=\n|von=1965\n|bis=1971\n|Architekt=Prosper Lemoine\n|Baustil=[[Moderne (Architektur)|Moderne]]\n|Offizielle Höhe=89\n|Etagen=23\n|Fläche=\n|Geschossfläche=\n|Baustoffe=[[Stahlbeton]], [[Stahl]], Fassade aus [[Glas]]\n|Rang_Stadt=13\n|Rang_Land=\n|Stadt=Berlin\n|Land=Deutschland\n|Kontinent=Europa\n}}\n\nDas heutige '''Postbank-Hochhaus''' (früher: '''Postscheckamt Berlin West''' (Bln W), seit 1985: '''Postgiroamt Berlin''') ist ein [[Hochhaus]] der [[Postbank]] am [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Hallesches Ufer*|Halleschen Ufer]] 40–60 und der [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Großbeerenstraße*|Großbeerenstraße]] 2 im [[Berlin]]er Ortsteil [[Berlin-Kreuzberg|Kreuzberg]].\n\n== Geschichte und Entstehung ==\nDas Postscheckamt von Berlin war ab 1909 in einem Neubau in der [[Dorotheenstraße (Berlin)|Dorotheenstraße]] 29 (heute: 84), der einen Teil der ehemaligen [[Markthalle IV]] integrierte, untergebracht und war bis zum Ende des [[Zweiter Weltkrieg|Zweiten Weltkriegs]] für den Bereich der Städte Berlin, [[Frankfurt (Oder)]], [[Potsdam]], [[Magdeburg]] und [[Stettin]] zuständig. Aufgrund der [[Deutsche Teilung|Deutschen Teilung]] wurde das Postscheckamt in der Dorotheenstraße nur noch von der [[Deutsche Post (DDR)|Deutschen Post der DDR]] genutzt. Für den [[West-Berlin|Westteil von Berlin]] gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das ''Postscheckamt West'' eröffnet. 2014 kaufte die ''CG-Gruppe'' das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden.<ref>[http://www.berliner-zeitung.de/berlin/kreuzberg-wohnen-im-postbank-tower-3269104 ''Kreuzberg: Wohnen im Postbank-Tower''.] In: ''[[Berliner Zeitung]]'', 7. Februar 2014</ref>\n\n== Architektur ==\n[[Datei:Gottfried Gruner - Springbrunnen.jpg|mini|Springbrunnenanlage von [[Gottfried Gruner]]]]\n\nNach den Plänen des Oberpostdirektors [[Prosper Lemoine]] wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23&nbsp;[[Geschoss (Architektur)|Geschosse]] und gehört mit einer Höhe von 89&nbsp;Metern bis heute zu den [[Liste der Hochhäuser in Berlin|höchsten Gebäuden in Berlin]]. Das Hochhaus besitzt eine [[Aluminium]]-Glas-Fassade und wurde im sogenannten „[[Internationaler Stil|Internationalen Stil]]“ errichtet. Die Gestaltung des Gebäudes orientiert sich an [[Mies van der Rohe]]s [[Seagram Building]] in [[New York City|New York]].\n\nZu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der ''Große Brunnen'' von [[Gottfried Gruner]]. Er besteht aus 18&nbsp;Säulen aus [[Bronze]] und wurde 1972 in Betrieb genommen.\n\n== UKW-Sender ==\nIm Postbank-Hochhaus befinden sich mehrere [[Ultrakurzwellensender|UKW-Sender]], die von [[Media Broadcast]] betrieben werden. Die [[Deutsche Funkturm]] (DFMG), eine Tochtergesellschaft der [[Deutsche Telekom|Deutschen Telekom AG]], stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u.&nbsp;a. folgende Hörfunkprogramme auf [[Ultrakurzwelle]] ausgestrahlt:\n* [[88vier]], 500-W-Sender auf 88,4 MHz\n* [[NPR Berlin]], 400-W-Sender auf 104,1 MHz\n* [[Radio Russkij Berlin]], 100-W-Sender auf 97,2 MHz\n\n== Siehe auch ==\n* [[Postscheckamt]]\n* [[Postgeschichte und Briefmarken Berlins]]\n* [[Liste von Sendeanlagen in Berlin]]\n\n== Literatur ==\n* ''Vom Amt aus gesehen – Postscheckamt Berlin West (Prosper Lemoine).'' In: ''[[Bauwelt (Zeitschrift)|Bauwelt]].'' 43/1971 (Thema: Verwaltungsgebäude).\n\n== Weblinks ==\n{{Commons|Postbank (Berlin)}}\n* [http://www.luise-berlin.de/lexikon/frkr/p/postgiroamt.htm Postscheckamt Berlin West auf der Seite des Luisenstädter Bildungsvereins]\n* [http://www.bildhauerei-in-berlin.de/_html/_katalog/details-1445.html Bildhauerei in Berlin: Vorstellung des großen Brunnens]\n\n== Einzelnachweise ==\n<references />\n\n{{Coordinate|article=/|NS=52.499626|EW=13.383655|type=landmark|region=DE-BE}}\n\n{{SORTIERUNG:Postbank Hochhaus Berlin}}\n[[Kategorie:Berlin-Kreuzberg]]\n[[Kategorie:Erbaut in den 1970er Jahren]]\n[[Kategorie:Bürogebäude in Berlin]]\n[[Kategorie:Bauwerk der Moderne in Berlin]]\n[[Kategorie:Berlin-Mitte]]\n[[Kategorie:Hochhaus in Berlin]]\n[[Kategorie:Postgeschichte (Deutschland)]]\n[[Kategorie:Landwehrkanal]]\n[[Kategorie:Hochhaus in Europa]]""")),
			WikipediaEntry("Postbank-Hochhaus Berlin", Option("""#WEITERLEITUNG [[Postbank-Hochhaus (Berlin)]]""")),
			WikipediaEntry("Abraham Lincoln", Option("""{{Infobox officeholder\n| vicepresident = [[Hannibal Hamlin]]\n}}\nAbraham Lincoln was the 16th [[President of the United States]].\n{{Infobox U.S. Cabinet\n| Vice President 2 = [[Andrew Johnson]]\n}}\n{{multiple image\n | direction=horizontal\n | width=\n | footer=\n | width1=180\n | image1=A&TLincoln.jpg\n | alt1=A seated Lincoln holding a book as his young son looks at it\n | caption1=1864 photo of President Lincoln with youngest son, [[Tad Lincoln|Tad]]\n | width2=164\n | image2=Mary Todd Lincoln 1846-1847 restored cropped.png\n | alt2=Black and white photo of Mary Todd Lincoln's shoulders and head\n | caption2=[[Mary Todd Lincoln]], wife of Abraham Lincoln, age 28\n }}"""))
		))
		// sc.getText()File("testwiki.txt").map(data => new WikipediaEntry("Audi", data))
	}
}
