import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex
import org.jsoup.Jsoup

class WikipediaTextparserTest extends FlatSpec with SharedSparkContext {
	"Wikipedia entry title" should "not change" in {
		wikipediaTestRDD()
			.map(entry => (entry.title, (entry, WikipediaTextparser.wikipediaToHtml(entry.text))))
			.map(tuple => (tuple._1, WikipediaTextparser.parseHtml(tuple._2).title))
			.collect
			.foreach(entry => assert(entry._1 == entry._2))
	}

	"Wikipedia article text" should "not contain wikimarkup" in {
		// matches [[...]] and {{...}} but not escaped '{', i.e. "\{"
		val wikimarkupRegex = new Regex("(\\[\\[.*?\\]\\])" + "|" + "([^\\\\]\\{\\{.*?\\}\\})")
		wikipediaTestRDD()
			.map(entry => WikipediaTextparser.wikipediaToHtml(entry.text))
			.collect
			.foreach(element => assert(wikimarkupRegex.findFirstIn(element).isEmpty))
	}

	"Wikipedia article text" should "contain none of the tags: table, h[0-9], small" in {
		val tagBlacklist = List[String]("table", "h[0-9]", "small")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.text)))
			.map(WikipediaTextparser.parseHtml)
			.map(_.text)
			.collect
			.foreach { element =>
				for(tag <- tagBlacklist) {
					val tagRegex = new Regex("(</" + tag + ">)|(<" + tag + "(>| .*?>))")
					assert(tagRegex.findFirstIn(element).isEmpty)
				}
			}
	}


	"Wikipedia article plain text" should "be complete" in {
		// term "Aussprache" should not be parsed because we filter WtTemplates
		val abstracts = Map(
			"Audi" -> """Die Audi AG (, Eigenschreibweise: AUDI AG) mit Sitz in Ingolstadt in Bayern ist ein deutscher Automobilhersteller, der dem Volkswagen-Konzern angehört. Der Markenname ist ein Wortspiel zur Umgehung der Namensrechte des ehemaligen Kraftfahrzeugherstellers A. Horch & Cie. Motorwagenwerke Zwickau.""",
			"Electronic Arts" -> """Electronic Arts (EA) ist ein börsennotierter, weltweit operierender Hersteller und Publisher von Computer- und Videospielen. Das Unternehmen wurde vor allem für seine Sportspiele (Madden NFL, FIFA) bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von Vivendi Games und Activision zu Activision Blizzard, war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt. Die Aktien des Unternehmens sind im Nasdaq Composite und im S&P 500 gelistet.""")
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.text)))
			.map(WikipediaTextparser.parseHtml)
			.map(element => (Jsoup.parse(element.text).body.text, abstracts(element.title)) )
			.collect
			.foreach(element => assert(element._1.startsWith(element._2)))
	}

	"Wikipedia links" should "not be empty" in {
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.text)))
			.map(WikipediaTextparser.parseHtml)
			.map(_.refs)
			.collect
			.foreach(element => assert(element.nonEmpty))
	}

	"Wikipedia text" should "contain exactly these links" in {
		val links = wikipediaTestReferences()
		wikipediaTestRDD()
			.map(entry => (entry, WikipediaTextparser.wikipediaToHtml(entry.text)))
			.map(WikipediaTextparser.parseHtml)
			.collect
			.foreach(entry =>
				assert(entry.refs == links(entry.title)))
	}

	// extracted links from Article abstracts
	def wikipediaTestReferences(): Map[String, Map[String, String]] = {
        Map(
			"Audi" -> Map("Ingolstadt" -> "Ingolstadt",
	            "Bayern" -> "Bayern",
	            "Automobilhersteller" -> "Automobilhersteller",
	            "Volkswagen" -> "Volkswagen AG",
	            "Wortspiel" -> "Wortspiel",
	            "Namensrechte" -> "Marke (Recht)",
	            "A. Horch & Cie. Motorwagenwerke Zwickau" -> "Horch",
	            "August Horch" -> "August Horch",
	            "Lateinische" -> "Latein",
	            "Imperativ" -> "Imperativ (Modus)",
	            "Zwickau" -> "Zwickau",
	            "Zschopauer" -> "Zschopau",
	            "DKW" -> "DKW",
	            "Wanderer" -> "Wanderer (Unternehmen)",
	            "Auto Union AG" -> "Auto Union",
	            "Chemnitz" -> "Chemnitz",
	            "Zweiten Weltkrieg" -> "Zweiter Weltkrieg",
	            "NSU Motorenwerke AG" -> "NSU Motorenwerke",
	            "Neckarsulm" -> "Neckarsulm"),

            "Electronic Arts" -> Map("Publisher" -> "Publisher",
                "Computer- und Videospielen" -> "Computerspiel",
                "Madden NFL" -> "Madden NFL",
                "FIFA" -> "FIFA (Spieleserie)",
                "Vivendi Games" -> "Vivendi Games",
                "Activision" -> "Activision",
                "Activision Blizzard" -> "Activision Blizzard",
                "Nasdaq Composite" -> "Nasdaq Composite",
                "S&P 500" -> "S&P 500"))
    }

	// extracted from Wikipedia
	def wikipediaTestRDD(): RDD[WikipediaTextparser.WikipediaEntry] = {
		sc.parallelize(List(
			WikipediaTextparser.WikipediaEntry("Audi", """{{Begriffsklärungshinweis}}\n{{Coordinate |NS=48/46/59.9808/N |EW=11/25/4.926/E |type=landmark |region=DE-BY }}\n{{Infobox Unternehmen\n| Name   = Audi AG\n| Logo   = Audi-Logo 2016.svg\n| Unternehmensform = [[Aktiengesellschaft]]\n| Gründungsdatum = 16. Juli 1909 in [[Zwickau]] (Audi)<br /><!--\n-->29. Juni 1932 in [[Chemnitz]] (Auto Union)<br /><!--\n-->3.&nbsp;September&nbsp;1949&nbsp;in&nbsp;[[Ingolstadt]]&nbsp;(Neugründung)<br /><!--\n-->10. März 1969 in [[Neckarsulm]] (Fusion)\n| ISIN   = DE0006757008\n| Sitz   = [[Ingolstadt]], [[Deutschland]]\n| Leitung  =\n* [[Rupert Stadler]],<br />[[Vorstand]]svorsitzender\n* [[Matthias Müller (Manager)|Matthias Müller]],<br />[[Aufsichtsrat]]svorsitzender\n| Mitarbeiterzahl = 82.838 <small>(31. Dez. 2015)</small><ref name="kennzahlen" />\n| Umsatz  = 58,42 [[Milliarde|Mrd.]] [[Euro|EUR]] <small>(2015)</small><ref name="kennzahlen" />\n| Branche  = [[Automobilhersteller]]\n| Homepage  = www.audi.de\n}}\n\n[[Datei:Audi Ingolstadt.jpg|mini|Hauptsitz in Ingolstadt]]\n[[Datei:Neckarsulm 20070725.jpg|mini|Audi-Werk in Neckarsulm (Bildmitte)]]\n[[Datei:Michèle Mouton, Audi Quattro A1 - 1983 (11).jpg|mini|Kühlergrill mit Audi-Emblem <small>[[Audi quattro]] (Rallye-Ausführung, Baujahr 1983)</small>]]\n[[Datei:Audi 2009 logo.svg|mini|Logo bis April 2016]]\n\nDie '''Audi AG''' ({{Audio|Audi AG.ogg|Aussprache}}, Eigenschreibweise: ''AUDI AG'') mit Sitz in [[Ingolstadt]] in [[Bayern]] ist ein deutscher [[Automobilhersteller]], der dem [[Volkswagen AG|Volkswagen]]-Konzern angehört.\n\nDer Markenname ist ein [[Wortspiel]] zur Umgehung der [[Marke (Recht)|Namensrechte]] des ehemaligen Kraftfahrzeugherstellers ''[[Horch|A. Horch & Cie. Motorwagenwerke Zwickau]]''. Unternehmensgründer [[August Horch]], der „seine“ Firma nach Zerwürfnissen mit dem Finanzvorstand verlassen hatte, suchte einen Namen für sein neues Unternehmen und fand ihn im Vorschlag des Zwickauer Gymnasiasten Heinrich Finkentscher (Sohn des mit A. Horch befreundeten Franz Finkentscher), der ''Horch'' ins [[Latein]]ische übersetzte.<ref>Film der Audi AG: ''Die Silberpfeile aus Zwickau.'' Interview mit August Horch, Video 1992.</ref> ''Audi'' ist der [[Imperativ (Modus)|Imperativ]] Singular von ''audire'' (zu Deutsch ''hören'', ''zuhören'') und bedeutet „Höre!“ oder eben „Horch!“. Am 25. April 1910 wurde die ''Audi Automobilwerke GmbH Zwickau'' in das Handelsregister der Stadt [[Zwickau]] eingetragen.\n\n1928 übernahm die [[Zschopau]]er ''Motorenwerke J. S. Rasmussen AG'', bekannt durch ihre Marke ''[[DKW]]'', die Audi GmbH. Audi wurde zur Tochtergesellschaft und 1932 mit der Übernahme der Horchwerke AG sowie einem Werk des Unternehmens ''[[Wanderer (Unternehmen)|Wanderer]]'' Teil der neu gegründeten ''[[Auto Union|Auto Union AG]]'' mit Sitz in [[Chemnitz]], die folglich die vier verschiedenen Marken unter einem Dach anboten. Daraus entstand auch das heutige aus vier Ringen bestehende Logo von Audi, das darin ursprünglich nur für einen der Ringe gestanden hatte.\n\nNach dem [[Zweiter Weltkrieg|Zweiten Weltkrieg]] wurde 1949 die ''Auto Union GmbH'' nun mit Sitz in [[Ingolstadt]] neugegründet. Nachdem diese sich zunächst auf die Marke ''DKW'' konzentriert hatte, wurde 1965 erstmals wieder die Marke ''Audi'' verwendet. Im Zuge der Fusion 1969 mit der ''[[NSU Motorenwerke|NSU Motorenwerke AG]]'' zur ''Audi NSU Auto Union AG'' wurde die Marke ''Audi'' zum ersten Mal nach 37 Jahren als prägender Bestandteil in den Firmennamen der Auto Union aufgenommen. Hauptsitz war, dem Fusionspartner entsprechend, bis 1985 in [[Neckarsulm]], bevor der Unternehmensname der ehemaligen Auto Union infolge des Auslaufens der Marke NSU auf ''Audi AG'' verkürzt wurde und der Sitz wieder zurück nach Ingolstadt wechselte.""", null),
			WikipediaTextparser.WikipediaEntry("Electronic Arts", """{{Infobox Unternehmen\n| Name             = Electronic Arts, Inc.\n| Logo             = [[Datei:Electronic-Arts-Logo.svg|200px]]\n| Unternehmensform = [[Gesellschaftsrecht der Vereinigten Staaten#Corporation|Corporation]]\n| ISIN             = US2855121099\n| Gründungsdatum   = 1982\n| Sitz             = [[Redwood City]], [[Vereinigte Staaten|USA]]\n| Leitung          = Andrew Wilson (CEO)<br />[[Larry Probst]] (Chairman)\n| Mitarbeiterzahl  = 9.300 (2013)<ref>Electronic Arts: [http://www.ea.com/about About EA]. Offizielle Unternehmenswebseite, zuletzt abgerufen am 31. Dezember 2013</ref>\n| Umsatz           = 3,575 Milliarden [[US-Dollar|USD]] <small>([[Geschäftsjahr|Fiskaljahr]] 2014)</small>\n| Branche          = [[Softwareentwicklung]]\n| Homepage         = [http://www.ea.com/de/ www.ea.com/de]\n}}\n'''Electronic Arts''' ('''EA''') ist ein börsennotierter, weltweit operierender Hersteller und [[Publisher]] von [[Computerspiel|Computer- und Videospielen]]. Das Unternehmen wurde vor allem für seine Sportspiele (''[[Madden NFL]]'', ''[[FIFA (Spieleserie)|FIFA]]'') bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von [[Vivendi Games]] und [[Activision]] zu [[Activision Blizzard]], war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt.<ref name="economist">''Looking forward to the next level. The world’s biggest games publisher sees good times ahead.'' In: ''The Economist.'' 8. Februar 2007, S.&nbsp;66.</ref> Die Aktien des Unternehmens sind im [[Nasdaq Composite]] und im [[S&P 500]] gelistet.""", null)
		))

		//sc.textFile("testwiki.txt").map(data => WikipediaTextparser.WikipediaEntry("Audi", data, null))
	}
}
