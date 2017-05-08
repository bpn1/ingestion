package de.hpi.ingestion.textmining

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI
import org.jsoup.nodes.Element

import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._

import scala.io.{BufferedSource, Source}

// scalastyle:off number.of.methods
// scalastyle:off line.size.limit
// scalastyle:off method.length
// scalastyle:off file.size.limit

object TestData {

	def testSentences(): List[String] = {
		List(
			"This is a test sentence.",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen.",
			"Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen.")
	}

	def tokenizedTestSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "in", "Hessen"),
			List("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine", "Links", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen"))
	}

	def filteredTokenizedSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "Ortsteilen", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "Hessen"),
			List("Audi", "Brachttal", "historisches", "Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "nochmal", "Hessen"))
	}

	def stemmedTokenizedSentences(): List[List[String]] = {
		List(
			List("thi", "is", "a", "test", "sentenc"),
			List("streitberg", "ist", "ein", "von", "sech", "ortsteil", "der", "gemei", "brachttal", "main-kinzig-kreis", "in", "hess"),
			List("link", "audi", "brachttal", "historisch", "jahr", "kein", "link", "hess", "main-kinzig-kreis", "buding", "wald", "backfisch", "und", "nochmal", "hess"))
	}

	def stemmedAndFilteredSentences(): List[List[String]] = {
		List(
			List("thi", "is", "a", "test", "sentenc"),
			List("streitberg", "ortsteil", "gemei", "brachttal", "main-kinzig-kreis", "hess"),
			List("audi", "brachttal", "historisch", "jahr", "hess", "main-kinzig-kreis", "buding", "wald", "backfisch", "nochmal", "hess"))
	}

	def reversedSentences(): List[String] = {
		List(
			"This is a test sentence",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal Main-Kinzig-Kreis in Hessen",
			"Links Audi Brachttal historisches Jahr Keine Links Hessen Main-Kinzig-Kreis Büdinger Wald Backfisch und nochmal Hessen")
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
			"historisches Jahr"))
			.sortBy(identity)
	}

	def parsedWikipediaTestSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi")),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List(),
				List(),
				List("Audi")),
			ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546))
				),
				List(),
				List("Streitberg", "Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald")),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24))
				),
				List(),
				List("Audi", "Brachttal", "historisches Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch")))
	}

	def articlesWithContextTestSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi"),
				context = Map("audi" -> 1, "verlink" -> 1)
			),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List(),
				List(),
				List("Audi"),
				context = Map("audi" -> 1, "verlink" -> 1)
			),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24))
				),
				List(),
				List("Audi", "Brachttal", "historisches Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"),
				context = Map("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
	}

	def linksWithContextsTestSet(): Set[Link] = {
		Set(
			Link("Audi", "Audi", Option(9), Map("verlink" -> 1)),
			Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
			Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
			Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
			Link("1377", "1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
			Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1)),
			Link("Audi", "Audi", Option(7), Map("brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
			Link("Brachttal", "Brachttal", Option(13), Map("audi" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
			Link("historisches Jahr", "1377", Option(24), Map("audi" -> 1, "brachttal" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
	}

	def articlesWithLinkContextsTestSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi"),
				linkswithcontext = List(Link("Audi", "Audi", Option(9), Map("verlink" -> 1)))
			),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List(),
				List(),
				List("Audi"),
				linkswithcontext = List()
			),
			ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546))
				),
				List(),
				List("Streitberg", "Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"),
				linkswithcontext = List(
					Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
					Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
					Link("1377", "1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1)))
			),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24))
				),
				List(),
				List("Audi", "Brachttal", "historisches Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"),
				linkswithcontext = List(
					Link("Audi", "Audi", Option(7), Map("brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
					Link("Brachttal", "Brachttal", Option(13), Map("audi" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
					Link("historisches Jahr", "1377", Option(24), Map("audi" -> 1, "brachttal" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
			))
	}

	def articlesWithCompleteContexts(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi"),
				linkswithcontext = List(Link("Audi", "Audi", Option(9), Map("verlink" -> 1))),
				context = Map("audi" -> 1, "verlink" -> 1)
			),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				List(),
				List(),
				List("Audi"),
				linkswithcontext = List(),
				context = Map("audi" -> 1, "verlink" -> 1)
			),
			ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546))
				),
				List(),
				List("Streitberg", "Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"),
				linkswithcontext = List(
					Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
					Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1)),
					Link("1377", "1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1))),
				context = Map("1554" -> 1, "waldrech" -> 1, "einwohnerzahl" -> 1, "streidtburgk" -> 1, "19" -> 1, "brachttal" -> 1, "ort" -> 1, "jahrhu" -> 1, "nachweislich" -> 1, "-lrb-" -> 3, "huterech" -> 1, "eingeburg" -> 1, "steytberg" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "-" -> 1, "stridberg" -> 1, "kleinst" -> 1, "-rrb-" -> 3, "stamm" -> 1, "hess" -> 1, "holx" -> 1, "buding" -> 1, "tauch" -> 1, "stripurgk" -> 1, "1500" -> 1, "gemei" -> 1, "1377" -> 1, "wald" -> 1, "main-kinzig-kreis" -> 1, "1528" -> 1, "namensvaria" -> 1, "ortsnam" -> 1, "streitberg" -> 2, "mittelal" -> 1, "red" -> 1)
			),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24))
				),
				List(),
				List("Audi", "Brachttal", "historisches Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"),
				linkswithcontext = List(
					Link("Audi", "Audi", Option(7), Map("brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
					Link("Brachttal", "Brachttal", Option(13), Map("audi" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
					Link("historisches Jahr", "1377", Option(24), Map("audi" -> 1, "brachttal" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1))),
				context = Map("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)
			))
	}


	def linkContextsTfidf(): Set[(Link, Map[String, Double])] = {
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
			(Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "hess" -> tf1df2, "gemei" -> tf1df1, "streitberg" -> tf1df1)),
			(Link("Hessen", "Hessen", Option(87)), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "gemei" -> tf1df1, "main-kinzig-kreis" -> tf1df2, "streitberg" -> tf1df1)),
			(Link("1377", "1377", Option(225)), Map("einwohnerzahl" -> tf1df1, "streidtburgk" -> tf1df1, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf1df1, "bezeichnung" -> tf1df1, "jahr" -> tf3df2, "270" -> tf1df1, "stridberg" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "tauch" -> tf1df1, "1500" -> tf1df1, "namensvaria" -> tf1df1, "red" -> tf1df1)),
			(Link("Büdinger Wald", "Büdinger Wald", Option(546)), Map("waldrech" -> tf1df1, "19" -> tf1df1, "ort" -> tf1df1, "jahrhu" -> tf1df1, "-lrb-" -> tf1df1, "huterech" -> tf1df1, "eingeburg" -> tf1df1, "-" -> tf1df1, "-rrb-" -> tf1df1, "holx" -> tf1df1, "ortsnam" -> tf1df1, "streitberg" -> tf1df1, "mittelal" -> tf1df1)),
			(Link("Audi", "Audi", Option(7)), Map("brachttal" -> tf1df2, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("Brachttal", "Brachttal", Option(13)), Map("audi" -> tf1df3, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("historisches Jahr", "1377", Option(24)), Map("audi" -> tf1df3, "brachttal" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)))
	}

	def termFrequenciesTestSet(): Set[(String, Bag[String, Int])] = {
		Set(
			("Audi Test mit Link", Bag("audi" -> 1, "verlink" -> 1)),
			("Audi Test ohne Link", Bag("audi" -> 1, "verlink" -> 1)),
			("Testartikel", Bag("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
	}

	def aliasOccurrencesInArticlesTestRDD(sc: SparkContext): RDD[AliasOccurrencesInArticle] = {
		sc.parallelize(List(
			AliasOccurrencesInArticle(Set("Audi"), Set()),
			AliasOccurrencesInArticle(Set(), Set("Audi")),
			AliasOccurrencesInArticle(Set("Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"), Set("Streitberg")),
			AliasOccurrencesInArticle(Set("Audi", "Brachttal", "historisches Jahr"), Set("Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"))
		))
	}

	def startAliasCounterTestRDD(sc: SparkContext): RDD[Alias] = {
		sc.parallelize(List(
			Alias("Audi", Map(), 1, 1),
			Alias("Audi", Map(), 1, 1),
			Alias("Audi", Map(), 0, 1),
			Alias("Brachttal", Map(), 1, 1),
			Alias("Brachttal", Map(), 1, 1),
			Alias("Main-Kinzig-Kreis", Map(), 1, 1),
			Alias("Main-Kinzig-Kreis", Map(), 0, 1),
			Alias("Hessen", Map(), 1, 1),
			Alias("Hessen", Map(), 0, 1),
			Alias("1377", Map(), 1, 1),
			Alias("Büdinger Wald", Map(), 1, 1),
			Alias("Büdinger Wald", Map(), 0, 1),
			Alias("Backfisch", Map(), 0, 1),
			Alias("Streitberg", Map(), 0, 1),
			Alias("historisches Jahr", Map(), 1, 1)
		))
	}

	def countedAliasesTestRDD(sc: SparkContext): RDD[Alias] = {
		sc.parallelize(List(
			Alias("Audi", Map(), 2, 3),
			Alias("Brachttal", Map(), 2, 2),
			Alias("Main-Kinzig-Kreis", Map(), 1, 2),
			Alias("Hessen", Map(), 1, 2),
			Alias("1377", Map(), 1, 1),
			Alias("Büdinger Wald", Map(), 1, 2),
			Alias("Backfisch", Map(), 0, 1),
			Alias("Streitberg", Map(), 0, 1),
			Alias("historisches Jahr", Map(), 1, 1)
		))
	}

	def linksSet(): Set[Alias] = {
		Set(
			Alias("Audi", Map("Audi" -> 2)),
			Alias("Brachttal", Map("Brachttal" -> 2)),
			Alias("Main-Kinzig-Kreis", Map("Main-Kinzig-Kreis" -> 1)),
			Alias("Hessen", Map("Hessen" -> 1)),
			Alias("1377", Map("1377" -> 1)),
			Alias("Büdinger Wald", Map("Büdinger Wald" -> 1)),
			Alias("historisches Jahr", Map("1337" -> 1))
		)
	}

	def aliasCountsSet(): Set[(String, Int, Int)] = {
		Set(
			("Audi", 2, 3),
			("Brachttal", 2, 2),
			("Main-Kinzig-Kreis", 1, 2),
			("Hessen", 1, 2),
			("1377", 1, 1),
			("Büdinger Wald", 1, 2),
			("Backfisch", 0, 1),
			("Streitberg", 0, 1),
			("historisches Jahr", 1, 1))
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
			("historisches Jahr", 1.0)))
			.sortBy(_._1)
	}

	def allPageNamesTestRDD(sc: SparkContext): RDD[String] = {
		sc.parallelize(List(
			"Brachttal",
			"Main-Kinzig-Kreis",
			"Hessen",
			"1377",
			"Büdinger Wald",
			"Audi"))
			.sortBy(identity)
	}

	def allPageNamesOfTestArticleList(): Set[String] = {
		Set(
			"Audi",
			"Brachttal",
			"1377")
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
			DocumentFrequency("audi", 3),
			DocumentFrequency("backfisch", 1)
		)
	}

	def filteredDocumentFrequenciesTestList(): List[DocumentFrequency] = {
		List(
			DocumentFrequency("audi", 3)
		)
	}

	def requestedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("backfisch", 2)
		)
	}

	def unstemmedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("Audi", 3),
			DocumentFrequency("Backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1),
			DocumentFrequency("einer", 2),
			DocumentFrequency("ein", 2),
			DocumentFrequency("Einer", 1),
			DocumentFrequency("Ein", 1))
	}

	def incorrectStemmedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		// This test case is wrong, since there are only 4 documents and "ein" has a document frequency of 6
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("backfisch", 1),
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 2),
			DocumentFrequency("zugleich", 1),
			DocumentFrequency("ein", 3)
		)
	}

	def stemmedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("ist", 3),
			DocumentFrequency("audi", 3),
			DocumentFrequency("hier", 2),
			DocumentFrequency("verlink", 2),
			DocumentFrequency("brachttal", 2),
			DocumentFrequency("main-kinzig-kreis", 2),
			DocumentFrequency("hess", 2),
			DocumentFrequency("buding", 2),
			DocumentFrequency("wald", 2),
			DocumentFrequency("jahr", 2),
			DocumentFrequency("und", 2),
			DocumentFrequency("nich", 1),
			DocumentFrequency("link", 1),
			DocumentFrequency("historisch", 1),
			DocumentFrequency("kein", 1),
			DocumentFrequency("backfisch", 1),
			DocumentFrequency("nochmal", 1))
	}

	def filteredStemmedDocumentFrequenciesTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("ist", 3),
			DocumentFrequency("audi", 3),
			DocumentFrequency("hier", 2),
			DocumentFrequency("verlink", 2),
			DocumentFrequency("brachttal", 2),
			DocumentFrequency("main-kinzig-kreis", 2),
			DocumentFrequency("hess", 2),
			DocumentFrequency("buding", 2),
			DocumentFrequency("wald", 2),
			DocumentFrequency("jahr", 2),
			DocumentFrequency("und", 2))
	}

	def filteredStemmedDocumentFrequenciesSmallTestSet(): Set[DocumentFrequency] = {
		// without article about Streitburg
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("ist", 2),
			DocumentFrequency("hier", 2),
			DocumentFrequency("verlink", 2))
	}

	def inverseDocumentFrequenciesTestSet(): Set[(String, Double)] = {
		val oneOccurrence = 0.6020599913279624
		val threeOccurrences = 0.12493873660829993
		Set(
			("audi", threeOccurrences),
			("backfisch", oneOccurrence))
	}

	def tfidfContextsTestSet(): Set[(String, Map[String, Double])] = {
		val tf1df1 = 0.47712125471966244
		val tf1df2 = 0.17609125905568124
		val tf2df1 = 0.9542425094393249
		val df3 = 0.0
		Set(
			("Audi Test mit Link", Map("audi" -> df3, "verlink" -> tf1df2)),
			("Audi Test ohne Link", Map("audi" -> df3, "verlink" -> tf1df2)),
			("Testartikel", Map("audi" -> df3, "brachttal" -> tf1df1, "historisch" -> tf1df1, "jahr" -> tf1df1, "hess" -> tf2df1, "main-kinzig-kreis" -> tf1df1, "buding" -> tf1df1, "wald" -> tf1df1, "backfisch" -> tf1df1, "nochmal" -> tf1df1)))
	}

	def unstemmedGermanWordsTestSet(): Set[String] = {
		Set("Es", "Keine", "Kein", "Keiner", "Ist", "keine", "keiner", "brauchen", "können", "sollte")
	}

	def wikipediaTestTextLinks(): Map[String, List[Link]] = {
		// extracted text links from article abstracts
		Map(
			"Audi" -> List(
				Link("Ingolstadt", "Ingolstadt", Option(55)),
				Link("Bayern", "Bayern", Option(69)),
				Link("Automobilhersteller", "Automobilhersteller", Option(94)),
				Link("Volkswagen", "Volkswagen AG", Option(123)),
				Link("Wortspiel", "Wortspiel", Option(175)),
				Link("Namensrechte", "Marke (Recht)", Option(202)),
				Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", Option(255)),
				Link("August Horch", "August Horch", Option(316)),
				Link("Lateinische", "Latein", Option(599)),
				Link("Imperativ", "Imperativ (Modus)", Option(636)),
				Link("Zwickau", "Zwickau", Option(829)),
				Link("Zschopauer", "Zschopau", Option(868)),
				Link("DKW", "DKW", Option(937)),
				Link("Wanderer", "Wanderer (Unternehmen)", Option(1071)),
				Link("Auto Union AG", "Auto Union", Option(1105)),
				Link("Chemnitz", "Chemnitz", Option(1131)),
				Link("Zweiten Weltkrieg", "Zweiter Weltkrieg", Option(1358)),
				Link("Ingolstadt", "Ingolstadt", Option(1423)),
				Link("NSU Motorenwerke AG", "NSU Motorenwerke", Option(1599)),
				Link("Neckarsulm", "Neckarsulm", Option(1830))),

			"Electronic Arts" -> List(
				Link("Publisher", "Publisher", Option(83)),
				Link("Computer- und Videospielen", "Computerspiel", Option(97)),
				Link("Madden NFL", "Madden NFL", Option(180)),
				Link("FIFA", "FIFA (Spieleserie)", Option(192)),
				Link("Vivendi Games", "Vivendi Games", Option(346)),
				Link("Activision", "Activision", Option(364)),
				Link("Activision Blizzard", "Activision Blizzard", Option(378)),
				Link("Nasdaq Composite", "Nasdaq Composite", Option(675)),
				Link("S&P 500", "S&P 500", Option(699))),

			"Abraham Lincoln" -> List(
				Link("President of the United States", "President of the United States", Option(29))),

			"Leadinstrument" -> List(
				Link("Leadinstrument", "Lead (Musik)")) // Redirect link
		)
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

	def wikipediaTestTemplateArticles(): Set[String] = {
		Set("Audi", "Electronic Arts", "Postbank-Hochhaus (Berlin)", "Abraham Lincoln")
	}

	def wikipediaEntries(): List[WikipediaEntry] = {
		// extracted from Wikipedia
		List(
			WikipediaEntry("Audi", Option("""{{Begriffsklärungshinweis}}\n{{Coordinate |NS=48/46/59.9808/N |EW=11/25/4.926/E |type=landmark |region=DE-BY }}\n{{Infobox Unternehmen\n| Name   = Audi AG\n| Logo   = Audi-Logo 2016.svg\n| Unternehmensform = [[Aktiengesellschaft]]\n| Gründungsdatum = 16. Juli 1909 in [[Zwickau]] (Audi)<br /><!--\n-->29. Juni 1932 in [[Chemnitz]] (Auto Union)<br /><!--\n-->3.&nbsp;September&nbsp;1949&nbsp;in&nbsp;[[Ingolstadt]]&nbsp;(Neugründung)<br /><!--\n-->10. März 1969 in [[Neckarsulm]] (Fusion)\n| ISIN   = DE0006757008\n| Sitz   = [[Ingolstadt]], [[Deutschland]]\n| Leitung  =\n* [[Rupert Stadler]],<br />[[Vorstand]]svorsitzender\n* [[Matthias Müller (Manager)|Matthias Müller]],<br />[[Aufsichtsrat]]svorsitzender\n| Mitarbeiterzahl = 82.838 <small>(31. Dez. 2015)</small><ref name="kennzahlen" />\n| Umsatz  = 58,42 [[Milliarde|Mrd.]] [[Euro|EUR]] <small>(2015)</small><ref name="kennzahlen" />\n| Branche  = [[Automobilhersteller]]\n| Homepage  = www.audi.de\n}}\n\n[[Datei:Audi Ingolstadt.jpg|mini|Hauptsitz in Ingolstadt]]\n[[Datei:Neckarsulm 20070725.jpg|mini|Audi-Werk in Neckarsulm (Bildmitte)]]\n[[Datei:Michèle Mouton, Audi Quattro A1 - 1983 (11).jpg|mini|Kühlergrill mit Audi-Emblem <small>[[Audi quattro]] (Rallye-Ausführung, Baujahr 1983)</small>]]\n[[Datei:Audi 2009 logo.svg|mini|Logo bis April 2016]]\n\nDie '''Audi AG''' ({{Audio|Audi AG.ogg|Aussprache}}, Eigenschreibweise: ''AUDI AG'') mit Sitz in [[Ingolstadt]] in [[Bayern]] ist ein deutscher [[Automobilhersteller]], der dem [[Volkswagen AG|Volkswagen]]-Konzern angehört.\n\nDer Markenname ist ein [[Wortspiel]] zur Umgehung der [[Marke (Recht)|Namensrechte]] des ehemaligen Kraftfahrzeugherstellers ''[[Horch|A. Horch & Cie. Motorwagenwerke Zwickau]]''. Unternehmensgründer [[August Horch]], der „seine“ Firma nach Zerwürfnissen mit dem Finanzvorstand verlassen hatte, suchte einen Namen für sein neues Unternehmen und fand ihn im Vorschlag des Zwickauer Gymnasiasten Heinrich Finkentscher (Sohn des mit A. Horch befreundeten Franz Finkentscher), der ''Horch'' ins [[Latein]]ische übersetzte.<ref>Film der Audi AG: ''Die Silberpfeile aus Zwickau.'' Interview mit August Horch, Video 1992.</ref> ''Audi'' ist der [[Imperativ (Modus)|Imperativ]] Singular von ''audire'' (zu Deutsch ''hören'', ''zuhören'') und bedeutet „Höre!“ oder eben „Horch!“. Am 25. April 1910 wurde die ''Audi Automobilwerke GmbH Zwickau'' in das Handelsregister der Stadt [[Zwickau]] eingetragen.\n\n1928 übernahm die [[Zschopau]]er ''Motorenwerke J. S. Rasmussen AG'', bekannt durch ihre Marke ''[[DKW]]'', die Audi GmbH. Audi wurde zur Tochtergesellschaft und 1932 mit der Übernahme der Horchwerke AG sowie einem Werk des Unternehmens ''[[Wanderer (Unternehmen)|Wanderer]]'' Teil der neu gegründeten ''[[Auto Union|Auto Union AG]]'' mit Sitz in [[Chemnitz]], die folglich die vier verschiedenen Marken unter einem Dach anboten. Daraus entstand auch das heutige aus vier Ringen bestehende Logo von Audi, das darin ursprünglich nur für einen der Ringe gestanden hatte.\n\nNach dem [[Zweiter Weltkrieg|Zweiten Weltkrieg]] wurde 1949 die ''Auto Union GmbH'' nun mit Sitz in [[Ingolstadt]] neugegründet. Nachdem diese sich zunächst auf die Marke ''DKW'' konzentriert hatte, wurde 1965 erstmals wieder die Marke ''Audi'' verwendet. Im Zuge der Fusion 1969 mit der ''[[NSU Motorenwerke|NSU Motorenwerke AG]]'' zur ''Audi NSU Auto Union AG'' wurde die Marke ''Audi'' zum ersten Mal nach 37 Jahren als prägender Bestandteil in den Firmennamen der Auto Union aufgenommen. Hauptsitz war, dem Fusionspartner entsprechend, bis 1985 in [[Neckarsulm]], bevor der Unternehmensname der ehemaligen Auto Union infolge des Auslaufens der Marke NSU auf ''Audi AG'' verkürzt wurde und der Sitz wieder zurück nach Ingolstadt wechselte.""")),
			WikipediaEntry("Electronic Arts", Option("""{{Infobox Unternehmen\n| Name             = Electronic Arts, Inc.\n| Logo             = [[Datei:Electronic-Arts-Logo.svg|200px]]\n| Unternehmensform = [[Gesellschaftsrecht der Vereinigten Staaten#Corporation|Corporation]]\n| ISIN             = US2855121099\n| Gründungsdatum   = 1982\n| Sitz             = [[Redwood City]], [[Vereinigte Staaten|USA]]\n| Leitung          = Andrew Wilson (CEO)<br />[[Larry Probst]] (Chairman)\n| Mitarbeiterzahl  = 9.300 (2013)<ref>Electronic Arts: [http://www.ea.com/about About EA]. Offizielle Unternehmenswebseite, zuletzt abgerufen am 31. Dezember 2013</ref>\n| Umsatz           = 3,575 Milliarden [[US-Dollar|USD]] <small>([[Geschäftsjahr|Fiskaljahr]] 2014)</small>\n| Branche          = [[Softwareentwicklung]]\n| Homepage         = [http://www.ea.com/de/ www.ea.com/de]\n}}\n'''Electronic Arts''' ('''EA''') ist ein börsennotierter, weltweit operierender Hersteller und [[Publisher]] von [[Computerspiel|Computer- und Videospielen]]. Das Unternehmen wurde vor allem für seine Sportspiele (''[[Madden NFL]]'', ''[[FIFA (Spieleserie)|FIFA]]'') bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von [[Vivendi Games]] und [[Activision]] zu [[Activision Blizzard]], war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt.<ref name="economist">''Looking forward to the next level. The world’s biggest games publisher sees good times ahead.'' In: ''The Economist.'' 8. Februar 2007, S.&nbsp;66.</ref> Die Aktien des Unternehmens sind im [[Nasdaq Composite]] und im [[S&P 500]] gelistet.""")),
			WikipediaEntry("Postbank-Hochhaus (Berlin)", Option("""{{Infobox Hohes Gebäude\n|Name=Postbank-Hochhaus Berlin\n|Bild=Postbank-Hochhaus Berlin.jpg|miniatur|Das Postbank-Hochhaus.\n|Ort=[[Berlin-Kreuzberg]]\n|Nutzung=Bürogebäude\n|Arbeitsplätze=\n|von=1965\n|bis=1971\n|Architekt=Prosper Lemoine\n|Baustil=[[Moderne (Architektur)|Moderne]]\n|Offizielle Höhe=89\n|Etagen=23\n|Fläche=\n|Geschossfläche=\n|Baustoffe=[[Stahlbeton]], [[Stahl]], Fassade aus [[Glas]]\n|Rang_Stadt=13\n|Rang_Land=\n|Stadt=Berlin\n|Land=Deutschland\n|Kontinent=Europa\n}}\n\nDas heutige '''Postbank-Hochhaus''' (früher: '''Postscheckamt Berlin West''' (Bln W), seit 1985: '''Postgiroamt Berlin''') ist ein [[Hochhaus]] der [[Postbank]] am [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Hallesches Ufer*|Halleschen Ufer]] 40–60 und der [[Liste der Straßen und Plätze in Berlin-Kreuzberg#Großbeerenstraße*|Großbeerenstraße]] 2 im [[Berlin]]er Ortsteil [[Berlin-Kreuzberg|Kreuzberg]].\n\n== Geschichte und Entstehung ==\nDas Postscheckamt von Berlin war ab 1909 in einem Neubau in der [[Dorotheenstraße (Berlin)|Dorotheenstraße]] 29 (heute: 84), der einen Teil der ehemaligen [[Markthalle IV]] integrierte, untergebracht und war bis zum Ende des [[Zweiter Weltkrieg|Zweiten Weltkriegs]] für den Bereich der Städte Berlin, [[Frankfurt (Oder)]], [[Potsdam]], [[Magdeburg]] und [[Stettin]] zuständig. Aufgrund der [[Deutsche Teilung|Deutschen Teilung]] wurde das Postscheckamt in der Dorotheenstraße nur noch von der [[Deutsche Post (DDR)|Deutschen Post der DDR]] genutzt. Für den [[West-Berlin|Westteil von Berlin]] gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das ''Postscheckamt West'' eröffnet. 2014 kaufte die ''CG-Gruppe'' das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden.<ref>[http://www.berliner-zeitung.de/berlin/kreuzberg-wohnen-im-postbank-tower-3269104 ''Kreuzberg: Wohnen im Postbank-Tower''.] In: ''[[Berliner Zeitung]]'', 7. Februar 2014</ref>\n\n== Architektur ==\n[[Datei:Gottfried Gruner - Springbrunnen.jpg|mini|Springbrunnenanlage von [[Gottfried Gruner]]]]\n\nNach den Plänen des Oberpostdirektors [[Prosper Lemoine]] wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23&nbsp;[[Geschoss (Architektur)|Geschosse]] und gehört mit einer Höhe von 89&nbsp;Metern bis heute zu den [[Liste der Hochhäuser in Berlin|höchsten Gebäuden in Berlin]]. Das Hochhaus besitzt eine [[Aluminium]]-Glas-Fassade und wurde im sogenannten „[[Internationaler Stil|Internationalen Stil]]“ errichtet. Die Gestaltung des Gebäudes orientiert sich an [[Mies van der Rohe]]s [[Seagram Building]] in [[New York City|New York]].\n\nZu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der ''Große Brunnen'' von [[Gottfried Gruner]]. Er besteht aus 18&nbsp;Säulen aus [[Bronze]] und wurde 1972 in Betrieb genommen.\n\n== UKW-Sender ==\nIm Postbank-Hochhaus befinden sich mehrere [[Ultrakurzwellensender|UKW-Sender]], die von [[Media Broadcast]] betrieben werden. Die [[Deutsche Funkturm]] (DFMG), eine Tochtergesellschaft der [[Deutsche Telekom|Deutschen Telekom AG]], stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u.&nbsp;a. folgende Hörfunkprogramme auf [[Ultrakurzwelle]] ausgestrahlt:\n* [[88vier]], 500-W-Sender auf 88,4 MHz\n* [[NPR Berlin]], 400-W-Sender auf 104,1 MHz\n* [[Radio Russkij Berlin]], 100-W-Sender auf 97,2 MHz\n\n== Siehe auch ==\n* [[Postscheckamt]]\n* [[Postgeschichte und Briefmarken Berlins]]\n* [[Liste von Sendeanlagen in Berlin]]\n\n== Literatur ==\n* ''Vom Amt aus gesehen – Postscheckamt Berlin West (Prosper Lemoine).'' In: ''[[Bauwelt (Zeitschrift)|Bauwelt]].'' 43/1971 (Thema: Verwaltungsgebäude).\n\n== Weblinks ==\n{{Commons|Postbank (Berlin)}}\n* [http://www.luise-berlin.de/lexikon/frkr/p/postgiroamt.htm Postscheckamt Berlin West auf der Seite des Luisenstädter Bildungsvereins]\n* [http://www.bildhauerei-in-berlin.de/_html/_katalog/details-1445.html Bildhauerei in Berlin: Vorstellung des großen Brunnens]\n\n== Einzelnachweise ==\n<references />\n\n{{Coordinate|article=/|NS=52.499626|EW=13.383655|type=landmark|region=DE-BE}}\n\n{{SORTIERUNG:Postbank Hochhaus Berlin}}\n[[Kategorie:Berlin-Kreuzberg]]\n[[Kategorie:Erbaut in den 1970er Jahren]]\n[[Kategorie:Bürogebäude in Berlin]]\n[[Kategorie:Bauwerk der Moderne in Berlin]]\n[[Kategorie:Berlin-Mitte]]\n[[Kategorie:Hochhaus in Berlin]]\n[[Kategorie:Postgeschichte (Deutschland)]]\n[[Kategorie:Landwehrkanal]]\n[[Kategorie:Hochhaus in Europa]]""")),
			WikipediaEntry("Postbank-Hochhaus Berlin", Option("""#WEITERLEITUNG [[Postbank-Hochhaus (Berlin)]]""")),
			WikipediaEntry("Abraham Lincoln", Option("""{{Infobox officeholder\n| vicepresident = [[Hannibal Hamlin]]\n}}\nAbraham Lincoln was the 16th [[President of the United States]].\n{{Infobox U.S. Cabinet\n| Vice President 2 = [[Andrew Johnson]]\n}}\n{{multiple image\n | direction=horizontal\n | width=\n | footer=\n | width1=180\n | image1=A&TLincoln.jpg\n | alt1=A seated Lincoln holding a book as his young son looks at it\n | caption1=1864 photo of President Lincoln with youngest son, [[Tad Lincoln|Tad]]\n | width2=164\n | image2=Mary Todd Lincoln 1846-1847 restored cropped.png\n | alt2=Black and white photo of Mary Todd Lincoln's shoulders and head\n | caption2=[[Mary Todd Lincoln]], wife of Abraham Lincoln, age 28\n }}""")),
			WikipediaEntry("Zerfall", Option("""'''Zerfall''' steht für:\n* Radioaktiver Zerfall, siehe [[Radioaktivität]]\n* das [[Zerfallsgesetz]] einer radioaktiven Substanz\n* Exponentieller Zerfall, siehe [[Exponentielles Wachstum]]\n* den [[Zerfall (Soziologie)|Zerfall]] gesellschaftlicher Strukturen \n* ein Album der Band Eisregen, siehe [[Zerfall (Album)]]\n* den Film [[Raspad – Der Zerfall]]\n\n'''Siehe auch:'''\n{{Wiktionary}}\n\n{{Begriffsklärung}}""")),
			WikipediaEntry("Fisch", Option("""'''Fisch''' steht für:\n\n* [[Fische]], im Wasser lebende Wirbeltiere \n* [[Speisefisch]], eine Lebensmittelkategorie\n* [[Fische (Sternbild)]]\n* [[Fische (Tierkreiszeichen)]]\n* [[Fisch (Christentum)]], religiöses Symbol\n* [[Fisch (Wappentier)]], gemeine Figur in der Heraldik\n\n'''Fisch''' ist der Name folgender Orte:\n\n* [[Fisch (Saargau)]], Ortsgemeinde in Rheinland-Pfalz\n\n{{Begriffsklärung}}""")),
			WikipediaEntry("Zwilling (Begriffsklärung)", Option("""'''Zwilling''' bezeichnet:\n\n* den biologischen Zwilling, siehe [[Zwillinge]]\n* einen Begriff aus der Kristallkunde, siehe [[Kristallzwilling]]\n* [[Zwilling J. A. Henckels]], einen Hersteller von Haushaltswaren mit Sitz in Solingen\n\nin der Astronomie und Astrologie:\n\n* ein Sternbild, siehe [[Zwillinge (Sternbild)]]\n* eines der Tierkreiszeichen, siehe [[Zwillinge (Tierkreiszeichen)]]\n\n{{Begriffsklärung}}""")),
			WikipediaEntry("Leadinstrument", Option("""#REDIRECT [[Lead (Musik)]]\n[[Kategorie:Musikinstrument nach Funktion]]""")),
			WikipediaEntry("Salzachtal", Option("""#WEITERLEITUNG [[Salzach#Salzachtal]]\n[[Kategorie:Tal im Land Salzburg]]\n[[Kategorie:Tal in Oberösterreich]]\n[[Kategorie:Tal in Bayern]]\n[[Kategorie:Salzach|!]]""")))
	}

	def wikipediaEntryWithHeadlines(): WikipediaEntry = {
		WikipediaEntry("Postbank-Hochhaus (Berlin) Kurz", Option("""Das heutige '''Postbank-Hochhaus''' (früher: '''Postscheckamt Berlin West''' (Bln W), seit 1985: '''Postgiroamt Berlin''') ist ein [[Hochhaus]] der [[Postbank]].\n\n== Geschichte und Entstehung ==\nDas Postscheckamt von Berlin war ab 1909 in einem Neubau in der [[Dorotheenstraße (Berlin)|Dorotheenstraße]] 29 (heute: 84).\n\n== Architektur ==\nDie Gestaltung des Gebäudes orientiert sich an [[Mies van der Rohe]]s [[Seagram Building]] in [[New York City|New York]].\n\n== UKW-Sender ==\nIm Postbank-Hochhaus befinden sich mehrere [[Ultrakurzwellensender|UKW-Sender]].\n\n== Siehe auch ==\n\n== Literatur ==\n\n== Weblinks ==\n\n== Einzelnachweise =="""))
	}

	def documentWithHeadlines(): String = {
		"""<html>
		  | <head></head>
		  | <body>
		  |  <p>Das heutige <b>Postbank-Hochhaus</b> (früher: <b>Postscheckamt Berlin West</b> (Bln W), seit 1985: <b>Postgiroamt Berlin</b>) ist ein <a href="/Hochhaus" title="Hochhaus">Hochhaus</a> der <a href="/Postbank" title="Postbank">Postbank</a>.</p>
		  |  <h2><span class="mw-headline" id="Geschichte_und_Entstehung">Geschichte und Entstehung</span></h2>
		  |  <p>Das Postscheckamt von Berlin war ab 1909 in einem Neubau in der <a href="/Dorotheenstra%C3%9Fe_(Berlin)" title="Dorotheenstraße (Berlin)">Dorotheenstraße</a> 29 (heute: 84).</p>
		  |  <h2><span class="mw-headline" id="Architektur">Architektur</span></h2>
		  |  <p>Die Gestaltung des Gebäudes orientiert sich an <a href="/Mies_van_der_Rohe" title="Mies van der Rohe">Mies van der Rohes</a> <a href="/Seagram_Building" title="Seagram Building">Seagram Building</a> in <a href="/New_York_City" title="New York City">New York</a>.</p>
		  |  <h2><span class="mw-headline" id="UKW-Sender">UKW-Sender</span></h2>
		  |  <p>Im Postbank-Hochhaus befinden sich mehrere <a href="/Ultrakurzwellensender" title="Ultrakurzwellensender">UKW-Sender</a>.</p>
		  |  <h2><span class="mw-headline" id="Siehe_auch">Siehe auch</span></h2>
		  |  <h2><span class="mw-headline" id="Literatur">Literatur</span></h2>
		  |  <h2><span class="mw-headline" id="Weblinks">Weblinks</span></h2>
		  |  <h2><span class="mw-headline" id="Einzelnachweise">Einzelnachweise</span></h2>
		  | </body>
		  |</html>""".stripMargin
	}

	def wikipediaEntryHeadlines(): Set[String] = {
		Set("Geschichte und Entstehung", "Architektur", "UKW-Sender", "Siehe auch", "Literatur", "Weblinks", "Einzelnachweise")
	}

	def parsedArticleTextWithHeadlines(): String = {
		"Das heutige Postbank-Hochhaus (früher: Postscheckamt Berlin West (Bln W), seit 1985: Postgiroamt Berlin) ist ein Hochhaus der Postbank. Geschichte und Entstehung Das Postscheckamt von Berlin war ab 1909 in einem Neubau in der Dorotheenstraße 29 (heute: 84). Architektur Die Gestaltung des Gebäudes orientiert sich an Mies van der Rohes Seagram Building in New York. UKW-Sender Im Postbank-Hochhaus befinden sich mehrere UKW-Sender. Siehe auch Literatur Weblinks Einzelnachweise"
	}

	def wikipediaEntriesForParsing(): List[WikipediaEntry] = {
		List(
			WikipediaEntry("Salzachtal", Option("""#WEITERLEITUNG [[Salzach#Salzachtal]]\n[[Kategorie:Tal im Land Salzburg]]\n[[Kategorie:Tal in Oberösterreich]]\n[[Kategorie:Tal in Bayern]]\n[[Kategorie:Salzach|!]]"""))
		)
	}

	def parsedWikipediaEntries(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				"Salzachtal",
				Option("""REDIRECT Salzach#Salzachtal Kategorie:Tal im Land Salzburg Kategorie:Tal in Oberösterreich Kategorie:Tal in Bayern !"""),
				textlinks = List(Link("Salzachtal", "Salzach#Salzachtal")),
				categorylinks = List(Link("Tal im Land Salzburg", "Tal im Land Salzburg"), Link("Tal in Oberösterreich", "Tal in Oberösterreich"), Link("Tal in Bayern", "Tal in Bayern"), Link("!", "Salzach")))
		)
	}

	def wikipediaDisambiguationPagesTestSet(): Set[String] = {
		Set("Zerfall", "Fisch", "Zwilling (Begriffsklärung)")
	}

	def wikipediaTestAbstracts(): Map[String, String] = {
		Map(
			"Audi" -> """Die Audi AG (, Eigenschreibweise: AUDI AG) mit Sitz in Ingolstadt in Bayern ist ein deutscher Automobilhersteller, der dem Volkswagen-Konzern angehört. Der Markenname ist ein Wortspiel zur Umgehung der Namensrechte des ehemaligen Kraftfahrzeugherstellers A. Horch & Cie. Motorwagenwerke Zwickau.""",
			"Electronic Arts" -> """Electronic Arts (EA) ist ein börsennotierter, weltweit operierender Hersteller und Publisher von Computer- und Videospielen. Das Unternehmen wurde vor allem für seine Sportspiele (Madden NFL, FIFA) bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von Vivendi Games und Activision zu Activision Blizzard, war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt. Die Aktien des Unternehmens sind im Nasdaq Composite und im S&P 500 gelistet."""
		)
	}

	def groupedAliasesSet(): Set[Alias] = {
		Set(
			Alias("Ingolstadt", Map("Ingolstadt" -> 1)),
			Alias("Bayern", Map("Bayern" -> 1)),
			Alias("Automobilhersteller", Map("Automobilhersteller" -> 1)),
			Alias("Radioaktiver Zerfall", Map("Radioaktivität" -> 1)),
			Alias("Zerfall", Map("Zerfall (Album)" -> 1, "Radioaktivität" -> 1))
		)
	}

	def groupedPagesSet(): Set[Page] = {
		Set(
			Page("Ingolstadt", Map("Ingolstadt" -> 1)),
			Page("Bayern", Map("Bayern" -> 1)),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1)),
			Page("Zerfall (Album)", Map("Zerfall" -> 1)),
			Page("Radioaktivität", Map("Zerfall" -> 1, "Radioaktiver Zerfall" -> 1))
		)
	}

	def cleanedGroupedPagesSet(): Set[Page] = {
		Set(
			Page("Ingolstadt", Map("Ingolstadt" -> 1)),
			Page("Bayern", Map("Bayern" -> 1)),
			Page("Automobilhersteller", Map("Automobilhersteller" -> 1)),
			Page("Zerfall (Album)", Map("Zerfall" -> 1))
		)
	}

	def groupedValidPagesSet(): Set[(String, Set[String])] = {
		Set(
			("Audi", Set("Ingolstadt", "Bayern", "Automobilhersteller", "Zerfall (Album)"))
		)
	}

	def probabilityReferences(): Map[String, Double] = {
		Map(
			"Ingolstadt" -> 0.0,
			"Bayern" -> 1.0,
			"Automobilhersteller" -> 0.0,
			"Zerfall" -> 0.0)
	}

	def allPagesTestList(): List[String] = {
		List(
			"Automobilhersteller",
			"Ingolstadt",
			"Bayern",
			"Zerfall (Album)")
	}

	def smallerParsedWikipediaList(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				"Audi",
				Option("dummy text"),
				List(
					Link("Ingolstadt", "Ingolstadt", Option(55)),
					Link("Bayern", "Bayern", Option(69)),
					Link("Automobilhersteller", "Automobilhersteller", Option(94)),
					Link("Zerfall", "Zerfall (Album)", Option(4711)),
					Link("Zerfall", "Radioaktivität", Option(4711)),
					Link("Radioaktiver Zerfall", "Radioaktivität", Option(4711)),
					Link("Zerfall", "Zerfall (Soziologie)", Option(4711)), // dead link
					Link("", "page name with empty alias", Option(4711)),
					Link("alias with empty page name", "", Option(4711))
				)))
	}

	def validLinkSet(): Set[Link] = {
		Set(
			Link("Ingolstadt", "Ingolstadt", Option(55)),
			Link("Bayern", "Bayern", Option(69)),
			Link("Automobilhersteller", "Automobilhersteller", Option(94)),
			Link("Zerfall", "Zerfall (Album)", Option(4711)),
			Link("Zerfall", "Radioaktivität", Option(4711)),
			Link("Radioaktiver Zerfall", "Radioaktivität", Option(4711))
		)
	}

	def parsedArticlesWithoutLinksSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Volvo", Option("lorem")),
			ParsedWikipediaEntry("Opel", Option("ipsum")),
			ParsedWikipediaEntry("Deutsche Bahn", Option("hat Verspätung"))
		)
	}

	def parsedWikipediaSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi",
				Option("dummy text"),
				List(
					Link("Ingolstadt", "Ingolstadt", Option(55)),
					Link("Bayern", "Bayern", Option(69)),
					Link("Automobilhersteller", "Automobilhersteller", Option(94)),
					Link("Zerfall", "Zerfall (Album)", Option(4711)),
					Link("Zerfall", "Zerfall (Soziologie)", Option(4711)), // dead link
					Link("", "page name with empty alias", Option(4711)),
					Link("alias with empty page name", "", Option(4711))
				)),
			ParsedWikipediaEntry("Volvo", Option("lorem")),
			ParsedWikipediaEntry("Opel", Option("ipsum")),
			ParsedWikipediaEntry("Deutsche Bahn", Option("hat Verspätung"))
		)
	}

	def closedParsedWikipediaSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi",
				Option("dummy text"),
				List(
					Link("Ingolstadt", "Ingolstadt", Option(55)),
					Link("Bayern", "Bayern", Option(69)),
					Link("Automobilhersteller", "Automobilhersteller", Option(94)),
					Link("Zerfall", "Zerfall (Album)", Option(4711)),
					Link("Zerfall", "Radioaktivität", Option(4711)),
					Link("Radioaktiver Zerfall", "Radioaktivität", Option(4711)),
					Link("Zerfall", "Zerfall (Soziologie)", Option(4711)), // dead link
					Link("", "page name with empty alias", Option(4711)),
					Link("alias with empty page name", "", Option(4711))
				)),
			ParsedWikipediaEntry("Ingolstadt", Option("lorem")),
			ParsedWikipediaEntry("Bayern", Option("lorem")),
			ParsedWikipediaEntry("Automobilhersteller", Option("lorem")),
			ParsedWikipediaEntry("Zerfall (Album)", Option("lorem")),
			ParsedWikipediaEntry("Radioaktivität", Option("lorem")),
			ParsedWikipediaEntry("Volvo", Option("lorem")),
			ParsedWikipediaEntry("Opel", Option("ipsum")),
			ParsedWikipediaEntry("Deutsche Bahn", Option("hat Verspätung"))
		)
	}

	def cleanedParsedWikipediaSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi",
				Option("dummy text"),
				List(
					Link("Ingolstadt", "Ingolstadt", Option(55)),
					Link("Bayern", "Bayern", Option(69)),
					Link("Automobilhersteller", "Automobilhersteller", Option(94)),
					Link("Zerfall", "Zerfall (Album)", Option(4711))
				)))
	}

	def cleanedClosedParsedWikipediaSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi",
				Option("dummy text"),
				List(
					Link("Ingolstadt", "Ingolstadt", Option(55)),
					Link("Bayern", "Bayern", Option(69)),
					Link("Automobilhersteller", "Automobilhersteller", Option(94)),
					Link("Zerfall", "Zerfall (Album)", Option(4711)),
					Link("Zerfall", "Radioaktivität", Option(4711)),
					Link("Radioaktiver Zerfall", "Radioaktivität", Option(4711))
				)))
	}

	def germanStopwordsTestSet(): Set[String] = {
		Set("der", "die", "das", "und", "als", "ist", "an", "am", "im", "dem", "des")
	}

	def unstemmedDFTestSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Audi Backfisch ist und zugleich einer ein Link."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi")
			),
			ParsedWikipediaEntry("Audi Test mit Link", Option("Audi Backfisch ist und zugleich einer Einer Link"),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi")
			),
			ParsedWikipediaEntry("Audi Test mit Link", Option("Audi Backfisch ist und zugleich Ein Link."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi")
			),
			ParsedWikipediaEntry("Audi Test mit Link", Option("Audi audi aUdi auDi."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi")
			)
		)
	}

	def stemmedDFTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 4),
			DocumentFrequency("backfisch", 3)
		)
	}

	def germanDFStopwordsTestSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 3)
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

	def parsedEntry(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Schwarzer Humor",
			Option("""Hickelkasten in Barcelona, Spanien: Der Sprung in den „Himmel“ ist in diesem Fall ein Sprung in den Tod. Hier hat sich jemand einen makabren Scherz erlaubt. Als schwarzer Humor wird Humor bezeichnet, der Verbrechen, Krankheit, Tod und ähnliche Themen, für die gewöhnlich eine Abhandlung in ernster Form erwartet wird, in satirischer oder bewusst verharmlosender Weise verwendet. Oft bezieht er sich auf Zeitthemen. Schwarzer Humor bedient sich häufig paradoxer Stilfiguren. Nicht selten löst er Kontroversen aus darüber, ob man sich über die genannten Dinge lustig machen dürfe und wo die Grenzen des guten Geschmacks lägen; besonders ist dies der Fall, wenn religiöse und sexuelle Themen und tragische Ereignisse zum Gegenstand genommen werden. In der darstellenden Kunst nennt man auf schwarzen Humor setzende Werke schwarze Komödien. Der Begriff wurde durch den Surrealisten André Breton erstmals 1940 in seiner Schrift Anthologie de l’humour noir näher umrissen, wird jedoch seit den 1960er Jahren zum Teil deutlich anders verstanden, indem Kennzeichen der Desillusion und des Nihilismus hinzutraten. In dem Vorwort seines Werkes nennt Breton unter anderem Quellen von Freud und Hegel, die seiner Meinung nach in die Begriffsentwicklung eingeflossen sind. Ursprünge des ‚schwarzen Humors‘ sah Breton in seiner Anthologie bei einigen Werken des irischen Satirikers Jonathan Swift wie Directions to Servants, A Modest Proposal, A Meditation on a Broom-Stick und einige seiner Aphorismen. In den öffentlichen Gebrauch kam der Begriff erst in den 1960er Jahren insbesondere im angloamerikanischen Raum (‚black humour‘) durch die Rezeption von Schriftstellern wie Nathanael West, Vladimir Nabokov und Joseph Heller. So gilt Catch-22 (1961) als ein bekanntes Beispiel dieser Stilart, in dem die Absurdität des Militarismus im Zweiten Weltkrieg satirisch überspitzt wurde. Weitere Beispiele sind Kurt Vonnegut, Slaughterhouse Five (1969), Thomas Pynchon, V. (1963) und Gravity’s Rainbow (1973), sowie im Film Stanley Kubrick’s Dr. Strangelove (1964) und im Absurden Theater insbesondere bei Eugène Ionesco zu finden. Der Begriff black comedy (dtsch. „schwarze Komödie“), der in der englischen Sprache schon für einige Stücke Shakespeares angewandt wurde, weist nach dem Lexikon der Filmbegriffe der Christian-Albrechts-Universität zu Kiel als Komödientyp durch „manchmal sarkastischen, absurden und morbiden ‚schwarzen‘ Humor“ aus, der sich sowohl auf „ernste oder tabuisierte Themen wie Krankheit, Behinderung, Tod, Krieg, Verbrechen“ wie auch auf „für sakrosankt gehaltene Dinge“ richten kann und dabei „auch vor politischen Unkorrektheiten, derben Späßen, sexuellen und skatologischen Anzüglichkeiten nicht zurückschreckt.“ Dabei stehe „hinter der Fassade zynischer Grenzüberschreitungen“ häufig ein „aufrichtiges Anliegen, falsche Hierarchien, Konventionen und Verlogenheiten innerhalb einer Gesellschaft mit den Mitteln filmischer Satire zu entlarven.“ Als filmische Beispiele werden angeführt: Robert Altmans M*A*S*H (USA 1970), Mike Nichols’ Catch-22 (USA 1970), nach Joseph Heller) sowie in der Postmoderne Quentin Tarantinos Pulp Fiction (USA 1994) und Lars von Triers Idioterne (Dänemark 1998). Der Essayist François Bondy schrieb 1971 in Die Zeit: „Der schwarze Humor ist nicht zu verwechseln mit dem ‚kranken Humor‘, der aus den Staaten kam, mit seinen ‚sick jokes‘“ und nannte als Beispiel den Witz: „Mama, ich mag meinen kleinen Bruder nicht. – Schweig, iß, was man dir vorsetzt“. Witz und Humor seien jedoch nicht dasselbe und letzteres „eine originale Geschichte in einer besonderen Tonart“. Humor im Sinne von einer – wie der Duden definiert – „vorgetäuschten Heiterkeit mit der jemand einer unangenehmen oder verzweifelten Lage, in der er sich befindet, zu begegnen“ versucht, nennt man auch Galgenhumor."""),
			List(
				Link("Hickelkasten", "Hickelkasten", Option(0)),
				Link("Humor", "Humor", Option(182)),
				Link("satirischer", "Satire", Option(321)),
				Link("paradoxer", "Paradoxon", Option(451)),
				Link("Stilfiguren", "Rhetorische Figur", Option(461)),
				Link("Kontroversen", "Kontroverse", Option(495)),
				Link("guten Geschmacks", "Geschmack (Kultur)", Option(601)),
				Link("Surrealisten", "Surrealismus", Option(865)),
				Link("André Breton", "André Breton", Option(878)),
				Link("Desillusion", "Desillusion", Option(1061)),
				Link("Nihilismus", "Nihilismus", Option(1081)),
				Link("Freud", "Sigmund Freud", Option(1173)),
				Link("Hegel", "Georg Wilhelm Friedrich Hegel", Option(1183)),
				Link("Anthologie", "Anthologie", Option(1314)),
				Link("Jonathan Swift", "Jonathan Swift", Option(1368)),
				Link("Directions to Servants", "Directions to Servants", Option(1387)),
				Link("A Modest Proposal", "A Modest Proposal", Option(1411)),
				Link("A Meditation on a Broom-Stick", "A Meditation on a Broom-Stick", Option(1430)),
				Link("Aphorismen", "Aphorismus", Option(1478)),
				Link("Nathanael West", "Nathanael West", Option(1663)),
				Link("Vladimir Nabokov", "Vladimir Nabokov", Option(1679)),
				Link("Joseph Heller", "Joseph Heller", Option(1700)),
				Link("Catch-22", "Catch-22", Option(1723)),
				Link("Kurt Vonnegut", "Kurt Vonnegut", Option(1893)),
				Link("Slaughterhouse Five", "Schlachthof 5 oder Der Kinderkreuzzug", Option(1908)),
				Link("Thomas Pynchon", "Thomas Pynchon", Option(1936)),
				Link("V.", "V.", Option(1952)),
				Link("Gravity’s Rainbow", "Die Enden der Parabel", Option(1966)),
				Link("Stanley Kubrick", "Stanley Kubrick", Option(2006)),
				Link("Dr. Strangelove", "Dr. Seltsam oder: Wie ich lernte, die Bombe zu lieben", Option(2024)),
				Link("Absurden Theater", "Absurdes Theater", Option(2054)),
				Link("Eugène Ionesco", "Eugène Ionesco", Option(2088)),
				Link("Shakespeares", "Shakespeare", Option(2222)),
				Link("Christian-Albrechts-Universität zu Kiel", "Christian-Albrechts-Universität zu Kiel", Option(2296)),
				Link("Komödientyp", "Komödie", Option(2340)),
				Link("sarkastischen", "Sarkasmus", Option(2368)),
				Link("absurden", "Absurdität", Option(2383)),
				Link("morbiden", "Morbidität", Option(2396)),
				Link("tabuisierte", "Tabuisierung", Option(2462)),
				Link("sakrosankt", "Sakrosankt", Option(2551)),
				Link("politischen Unkorrektheiten", "Politische Korrektheit", Option(2612)),
				Link("sexuellen und skatologischen", "Vulgärsprache", Option(2656)),
				Link("zynischer", "Zynismus", Option(2756)),
				Link("Satire", "Satire", Option(2933)),
				Link("Robert Altmans", "Robert Altman", Option(2997)),
				Link("M*A*S*H", "MASH (Film)", Option(3012)),
				Link("Mike Nichols", "Mike Nichols", Option(3032)),
				Link("Catch-22", "Catch-22 – Der böse Trick", Option(3046)),
				Link("Joseph Heller", "Joseph Heller", Option(3071)),
				Link("Postmoderne", "Postmoderne", Option(3099)),
				Link("Quentin Tarantinos", "Quentin Tarantino", Option(3111)),
				Link("Pulp Fiction", "Pulp Fiction", Option(3130)),
				Link("Lars von Triers", "Lars von Trier", Option(3158)),
				Link("Idioterne", "Idioten", Option(3174)),
				Link("François Bondy", "François Bondy", Option(3214)),
				Link("Die Zeit", "Die Zeit", Option(3245)),
				Link("Witz", "Witz", Option(3491)),
				Link("Duden", "Duden", Option(3639)),
				Link("Galgenhumor", "Galgenhumor", Option(3806))
			))
	}

	def parsedEntryWithDifferentLinkTypes(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Origineller Titel",
			Option("In diesem Text könnten ganz viele verschiedene Links stehen."),
			textlinks = List(Link("Apfel", "Apfel", Option(0)), Link("Baum", "Baum", Option(4))),
			templatelinks = List(Link("Charlie", "Charlie C.")),
			categorylinks = List(Link("Dora", "Dora")),
			listlinks = List(Link("Fund", "Fund"), Link("Grieß", "Brei")),
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
			Link("Esel", "Esel")
		)
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
			Link("August Horch", "August Horch", Option(0)),
			Link("August Horch", "Benutzer:August Horch", Option(0)),
			Link("August Horch", "Thema:August Horch", Option(0)),
			Link("August Horch", "HD:August Horch", Option(0)),
			Link("August Horch", "Kategorie:August Horch", Option(0)))
	}

	def testListLinkPage(): String = {
		Source.fromURL(getClass.getResource("/textmining/test_data.html")).getLines().mkString("\n")
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
			Link("August Horch", "August Horch", Option(0)),
			Link("August Horch", "Kategorie:August Horch", Option(0)))
	}

	def testCategoryLinks(): List[Link] = {
		List(
			Link("Ingolstadt", "Ingolstadt", Option(55)),
			Link("Bayern", "Bayern", Option(69)),
			Link("Automobilhersteller", "Kategorie:Automobilhersteller", Option(94)),
			Link("Volkswagen", "Volkswagen AG", Option(123)),
			Link("Wortspiel", "Kategorie:Wortspiel", Option(175)),
			Link("Kategorie:Namensrechte", "Marke (Recht)", Option(202)),
			Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", Option(255)),
			Link("August Horch", "August Horch", Option(316)),
			Link("Lateinische", "Latein", Option(599)),
			Link("Imperativ", "Imperativ (Modus)", Option(636)),
			Link("Kategorie:Zwickau", "Zwickau", Option(829)),
			Link("Zschopauer", "Zschopau", Option(868)))
	}

	def testCleanedCategoryLinks(): List[Link] = {
		List(
			Link("Ingolstadt", "Ingolstadt", Option(55)),
			Link("Bayern", "Bayern", Option(69)),
			Link("Volkswagen", "Volkswagen AG", Option(123)),
			Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", Option(255)),
			Link("August Horch", "August Horch", Option(316)),
			Link("Lateinische", "Latein", Option(599)),
			Link("Imperativ", "Imperativ (Modus)", Option(636)),
			Link("Zschopauer", "Zschopau", Option(868)))
	}

	def testExtractedCategoryLinks(): List[Link] = {
		List(
			Link("Automobilhersteller", "Automobilhersteller"),
			Link("Wortspiel", "Wortspiel"),
			Link("Namensrechte", "Marke (Recht)"),
			Link("Zwickau", "Zwickau"))
	}

	def testRedirectDict(): Map[String, String] = {
		Map("Postbank-Hochhaus Berlin" -> "Postbank-Hochhaus (Berlin)")
	}

	def testLinksWithRedirects(): Set[Link] = {
		Set(Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus Berlin", Option(10)))
	}

	def testLinksWithResolvedRedirects(): Set[Link] = {
		Set(Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus (Berlin)", Option(10)))
	}

	def testEntriesWithBadRedirects(): List[WikipediaEntry] = {
		List(WikipediaEntry("Postbank-Hochhaus Berlin", Option("""#redirect [[Postbank-Hochhaus (Berlin)]]""")))
	}

	def document(): String = {
		"""<body><p><a href="/2._Juli" title="2. Juli">2. Juli</a>
		  |<a href="/1852" title="1852">1852</a> im Stadtteil
		  |<span class="math">bli</span> bla
		  |<span class="math">blub</span>
		  |<abbr title="Naturgesetze für Information">NGI</abbr>
		  |</p></body>""".stripMargin
	}

	def parsedEntriesWithRedirects(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Test Entry 1", Option("This is a test page"), textlinks = List(Link("alias 1", "Test Redirect Entry 1"))),
			ParsedWikipediaEntry("Test Entry 2", Option("This is another test page"), textlinks = List(Link("alias 2", "Test Redirect Entry 2"), Link("alias 3", "EU"))),
			ParsedWikipediaEntry("Test Redirect Entry 1", Option("REDIRECT test page"), textlinks = List(Link("Test Redirect Entry 1", "test page"))),
			ParsedWikipediaEntry("Test Redirect Entry 2", Option("REDIRECTtest page 2"), textlinks = List(Link("Test Redirect Entry 2", "test page 2"))),
			ParsedWikipediaEntry("Test Wrong Redirect Entry 1", Option("WEITERLEITUNG test page")),
			ParsedWikipediaEntry("EU", Option("REDIRECT Europäische Union"), textlinks = List(Link("EU", "Europäische Union")), foundaliases = List("REDIRECT"))
		)
	}

	def parsedEntriesWithResolvedRedirects(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Test Entry 1", Option("This is a test page"), textlinks = List(Link("alias 1", "test page"))),
			ParsedWikipediaEntry("Test Entry 2", Option("This is another test page"), textlinks = List(Link("alias 2", "test page 2"), Link("alias 3", "Europäische Union"))),
			ParsedWikipediaEntry("Test Redirect Entry 1", Option("REDIRECT test page"), textlinks = List(Link("Test Redirect Entry 1", "test page"))),
			ParsedWikipediaEntry("Test Redirect Entry 2", Option("REDIRECTtest page 2"), textlinks = List(Link("Test Redirect Entry 2", "test page 2"))),
			ParsedWikipediaEntry("Test Wrong Redirect Entry 1", Option("WEITERLEITUNG test page")),
			ParsedWikipediaEntry("EU", Option("REDIRECT Europäische Union"), textlinks = List(Link("EU", "Europäische Union")), foundaliases = List("REDIRECT"))
		)
	}

	def parsedRedirectEntries(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Test Redirect Entry 1", Option("REDIRECT test page"), textlinks = List(Link("Test Redirect Entry 1", "test page"))),
			ParsedWikipediaEntry("Test Redirect Entry 2", Option("REDIRECTtest page 2"), textlinks = List(Link("Test Redirect Entry 2", "test page 2"))),
			ParsedWikipediaEntry("EU", Option("REDIRECT Europäische Union"), textlinks = List(Link("EU", "Europäische Union")), foundaliases = List("REDIRECT")))
	}

	def redirectDict(): Map[String, String] = {
		Map(
			"Test Redirect Entry 1" -> "test page",
			"Test Redirect Entry 2" -> "test page 2",
			"EU" -> "Europäische Union")
	}

	def redirectMap(): Map[String, String] = {
		Map(
			"Site 1" -> "Site 3",
			"Site 3" -> "Site 6",
			"Site 4" -> "Site 5",
			"Site 5" -> "Site 4",
			"Site 7" -> "Site 7",
			"EU" -> "Europäische Union")
	}

	def cleanedRedirectMap(): Map[String, String] = {
		Map(
			"Site 1" -> "Site 6",
			"Site 3" -> "Site 6",
			"EU" -> "Europäische Union")
	}

	def entriesWithRedirects(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Test Entry 1",
				textlinks = List(Link("Alias 1", "Site 1")),
				disambiguationlinks = List(Link("Alias 2", "Site 2")),
				listlinks = List(Link("Alias 3", "Site 3")),
				categorylinks = List(Link("Alias 4", "Site 4")),
				templatelinks = List(Link("Alias 5", "Site 5"))),
			ParsedWikipediaEntry("Test Entry 2", textlinks = List(Link("Alias 6", "Site 2"))),
			ParsedWikipediaEntry("Rapunzel Naturkost", listlinks = List(Link("EU", "EU"))))
	}

	def entriesWithResolvedRedirects(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Test Entry 1",
				textlinks = List(Link("Alias 1", "Site 6")),
				disambiguationlinks = List(Link("Alias 2", "Site 2")),
				listlinks = List(Link("Alias 3", "Site 6")),
				categorylinks = List(Link("Alias 4", "Site 4")),
				templatelinks = List(Link("Alias 5", "Site 5"))),
			ParsedWikipediaEntry("Test Entry 2", textlinks = List(Link("Alias 6", "Site 2"))),
			ParsedWikipediaEntry("Rapunzel Naturkost", listlinks = List(Link("EU", "Europäische Union"))))
	}

	def aliasFileStream(): BufferedSource = {
		Source.fromURL(getClass.getResource("/textmining/aliasfile"))
	}

	def deserializedTrie(): TrieNode = {
		val trie = new TrieNode()
		val tokenizer = IngestionTokenizer(false, false)
		val aliasList = List("test alias abc", "this is a second alias", "a third alias", "test alias bcs", "this is a different sentence")
		for(alias <- aliasList) {
			trie.append(tokenizer.process(alias))
		}
		trie
	}

	def testDataTrie(tokenizer: IngestionTokenizer): TrieNode = {
		val trie = new TrieNode()
		val aliasList = Source.fromURL(getClass.getResource("/textmining/triealiases")).getLines()
		for(alias <- aliasList) {
			trie.append(tokenizer.process(alias))
		}
		trie
	}

	def uncleanedFoundAliaes(): List[String] = {
		List("Alias 1", "", "Alias 2", "Alias 1", "Alias 3")
	}

	def cleanedFoundAliaes(): List[String] = {
		List("Alias 1", "Alias 2", "Alias 3")
	}

	def binaryTrieStream(): ByteArrayInputStream = {
		val aliasStream = TestData.aliasFileStream()
		val trieStream = new ByteArrayOutputStream(1024)
		LocalTrieBuilder.serializeTrie(aliasStream, trieStream)
		new ByteArrayInputStream(trieStream.toByteArray)
	}

	def linkExtenderPagesSet(): Set[Page] = {
		Set(
			Page("Audi", Map("Audi AG" -> 10, "Audi" -> 10, "VW" -> 1)),
			Page("Bayern", Map("Bayern" -> 1)),
			Page("VW", Map("Volkswagen AG" -> 1, "VW" -> 1)),
			Page("Zerfall (Album)", Map("Zerfall" -> 1))
		)
	}

	def linkExtenderPagesMap(): Map[String, Map[String, Int]] = {
		Map(
			"Audi" -> Map("Audi AG" -> 10, "Audi" -> 10, "VW" -> 1),
			"Bayern" -> Map("Bayern" -> 1),
			"VW" -> Map("Volkswagen AG" -> 1, "VW" -> 1),
			"Zerfall (Album)" -> Map("Zerfall" -> 1)
		)
	}

	def linkExtenderParsedEntry(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry("Audi", Option("Audi ist Audi AG. VW ist Volkswagen AG"),
			List(
				Link("VW", "VW", Option(18))
			)
		)
	}

	def linkExtenderFoundPages(): Map[String, Map[String, Int]] = {
		Map(
			"Audi" -> Map("Audi AG" -> 10, "Audi" -> 10, "VW" -> 1),
			"VW" -> Map("Volkswagen AG" -> 1, "VW" -> 1)
		)
	}

	def linkExtenderFoundAliases(): Map[String, (String, String)] = {
		Map(
			"Audi" -> ("Audi", "Audi"),
			"Audi AG" -> ("Audi AG", "Audi"),
			"VW" -> ("VW", "VW"),
			"Volkswagen AG" -> ("Volkswagen AG", "VW")
		)
	}

	def linkExtenderExtendedParsedEntry(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi",
				Option("Audi ist Audi AG. VW ist Volkswagen AG"),
				List(Link("VW", "VW", Option(18), Map())),
				extendedlinks = List(
					Link("Audi", "Audi", Option(0)),
					Link("Audi AG", "Audi", Option(9)),
					Link("VW", "VW", Option(18)),
					Link("Volkswagen AG", "VW", Option(25))
				)
			)
		)
	}

	def linkExtenderTrie(tokenizer: IngestionTokenizer): TrieNode = {
		val trie = new TrieNode()
		val aliasList = List("Audi", "Audi AG", "VW", "Volkswagen AG")
		for(alias <- aliasList) {
			trie.append(tokenizer.process(alias))
		}
		trie
	}

	def bigLinkExtenderParsedEntry(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Postbank-Hochhaus (Berlin)",
			Option("Das heutige Postbank-Hochhaus (früher: Postscheckamt Berlin West (Bln W), seit 1985: Postgiroamt Berlin) ist ein Hochhaus der Postbank am Halleschen Ufer 40–60 und der Großbeerenstraße 2 im Berliner Ortsteil Kreuzberg. Das Postscheckamt von Berlin war ab 1909 in einem Neubau in der Dorotheenstraße 29 (heute: 84), der einen Teil der ehemaligen Markthalle IV integrierte, untergebracht und war bis zum Ende des Zweiten Weltkriegs für den Bereich der Städte Berlin, Frankfurt (Oder), Potsdam, Magdeburg und Stettin zuständig. Aufgrund der Deutschen Teilung wurde das Postscheckamt in der Dorotheenstraße nur noch von der Deutschen Post der DDR genutzt. Für den Westteil von Berlin gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das Postscheckamt West eröffnet. 2014 kaufte die CG-Gruppe das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden. Gottfried Gruner Nach den Plänen des Oberpostdirektors Prosper Lemoine wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23 Geschosse und gehört mit einer Höhe von 89 Metern bis heute zu den höchsten Gebäuden in Berlin. Das Hochhaus besitzt eine Aluminium-Glas-Fassade und wurde im sogenannten „Internationalen Stil“ errichtet. Die Gestaltung des Gebäudes orientiert sich an Mies van der Rohes Seagram Building in New York. Zu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der Große Brunnen von Gottfried Gruner. Er besteht aus 18 Säulen aus Bronze und wurde 1972 in Betrieb genommen. Im Postbank-Hochhaus befinden sich mehrere UKW-Sender, die von Media Broadcast betrieben werden. Die Deutsche Funkturm (DFMG), eine Tochtergesellschaft der Deutschen Telekom AG, stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u. a. folgende Hörfunkprogramme auf Ultrakurzwelle ausgestrahlt:"),
			List(
				Link("Hochhaus", "Hochhaus", Option(113)),
				Link("Postbank", "Postbank", Option(126)),
				Link("Berliner", "Berlin", Option(190)),
				Link("Kreuzberg", "Berlin-Kreuzberg", Option(208)),
				Link("Frankfurt (Oder)", "Frankfurt (Oder)", Option(465)),
				Link("Potsdam", "Potsdam", Option(483)),
				Link("Magdeburg", "Magdeburg", Option(492)),
				Link("Stettin", "Stettin", Option(506)),
				Link("Deutschen Post der DDR", "Deutsche Post (DDR)", Option(620)),
				Link("New York", "New York City", Option(1472)),
				Link("Bronze", "Bronze", Option(1800)),
				Link("Media Broadcast", "Media Broadcast", Option(1906)),
				Link("Deutsche Funkturm", "Deutsche Funkturm", Option(1944)),
				Link("Deutschen Telekom AG", "Deutsche Telekom", Option(1999))
			)
		)
	}

	def bigLinkExtenderPagesSet(): Set[Page] = {
		Set(
			Page("Audi", Map("Audi AG" -> 10, "Audi" -> 10, "VW" -> 2)),
			Page("VW", Map("Volkswagen AG" -> 2, "VW" -> 2)),
			Page("Hochhaus", Map("Hochhaus" -> 2, "Gebäude" -> 1)),
			Page("Postbank", Map("Postbank" -> 2)),
			Page("Berlin", Map("Berlin" -> 2, "(" -> 1, "Berliner" -> 2)),
			Page("Berlin-Kreuzberg", Map("Kreuzberg" -> 2)),
			Page("Frankfurt (Oder)", Map("Frankfurt (Oder)" -> 2)),
			Page("Potsdam", Map("Potsdam" -> 2)),
			Page("Magdeburg", Map("Magdeburg" -> 2)),
			Page("Stettin", Map("Stettin" -> 2)),
			Page("Deutsche Post (DDR)", Map("Deutschen Post der DDR" -> 2)),
			Page("New York City", Map("New York" -> 2)),
			Page("Bronze", Map("Bronze" -> 2)),
			Page("Media Broadcast", Map("Media Broadcast" -> 2)),
			Page("Deutsche Funkturm", Map("Deutsche Funkturm" -> 2, "DFMG" -> 11)),
			Page("Deutsche Telekom", Map("Deutschen Telekom AG" -> 2))
		)
	}

	def bigLinkExtenderExtendedParsedEntry(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Postbank-Hochhaus (Berlin)",
				Option("Das heutige Postbank-Hochhaus (früher: Postscheckamt Berlin West (Bln W), seit 1985: Postgiroamt Berlin) ist ein Hochhaus der Postbank am Halleschen Ufer 40–60 und der Großbeerenstraße 2 im Berliner Ortsteil Kreuzberg. Das Postscheckamt von Berlin war ab 1909 in einem Neubau in der Dorotheenstraße 29 (heute: 84), der einen Teil der ehemaligen Markthalle IV integrierte, untergebracht und war bis zum Ende des Zweiten Weltkriegs für den Bereich der Städte Berlin, Frankfurt (Oder), Potsdam, Magdeburg und Stettin zuständig. Aufgrund der Deutschen Teilung wurde das Postscheckamt in der Dorotheenstraße nur noch von der Deutschen Post der DDR genutzt. Für den Westteil von Berlin gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das Postscheckamt West eröffnet. 2014 kaufte die CG-Gruppe das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden. Gottfried Gruner Nach den Plänen des Oberpostdirektors Prosper Lemoine wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23 Geschosse und gehört mit einer Höhe von 89 Metern bis heute zu den höchsten Gebäuden in Berlin. Das Hochhaus besitzt eine Aluminium-Glas-Fassade und wurde im sogenannten „Internationalen Stil“ errichtet. Die Gestaltung des Gebäudes orientiert sich an Mies van der Rohes Seagram Building in New York. Zu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der Große Brunnen von Gottfried Gruner. Er besteht aus 18 Säulen aus Bronze und wurde 1972 in Betrieb genommen. Im Postbank-Hochhaus befinden sich mehrere UKW-Sender, die von Media Broadcast betrieben werden. Die Deutsche Funkturm (DFMG), eine Tochtergesellschaft der Deutschen Telekom AG, stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u. a. folgende Hörfunkprogramme auf Ultrakurzwelle ausgestrahlt:"),
				List(
					Link("Hochhaus", "Hochhaus", Option(113)),
					Link("Postbank", "Postbank", Option(126)),
					Link("Berliner", "Berlin", Option(190)),
					Link("Kreuzberg", "Berlin-Kreuzberg", Option(208)),
					Link("Frankfurt (Oder)", "Frankfurt (Oder)", Option(465)),
					Link("Potsdam", "Potsdam", Option(483)),
					Link("Magdeburg", "Magdeburg", Option(492)),
					Link("Stettin", "Stettin", Option(506)),
					Link("Deutschen Post der DDR", "Deutsche Post (DDR)", Option(620)),
					Link("New York", "New York City", Option(1472)),
					Link("Bronze", "Bronze", Option(1800)),
					Link("Media Broadcast", "Media Broadcast", Option(1906)),
					Link("Deutsche Funkturm", "Deutsche Funkturm", Option(1944)),
					Link("Deutschen Telekom AG", "Deutsche Telekom", Option(1999))
				),
				extendedlinks = List(
					Link("Berlin", "Berlin", Option(53)),
					Link("Berlin", "Berlin", Option(97)),
					Link("Hochhaus", "Hochhaus", Option(113)),
					Link("Postbank", "Postbank", Option(126)),
					Link("Berliner", "Berlin", Option(190)),
					Link("Kreuzberg", "Berlin-Kreuzberg", Option(208)),
					Link("Berlin", "Berlin", Option(241)),
					Link("Berlin", "Berlin", Option(457)),
					Link("Frankfurt (Oder)", "Frankfurt (Oder)", Option(465)),
					Link("Potsdam", "Potsdam", Option(483)),
					Link("Magdeburg", "Magdeburg", Option(492)),
					Link("Stettin", "Stettin", Option(506)),
					Link("Deutschen Post der DDR", "Deutsche Post (DDR)", Option(620)),
					Link("Berlin", "Berlin", Option(673)),
					Link("Gebäude", "Hochhaus", Option(818)),
					Link("Postbank", "Postbank", Option(834)),
					Link("Gebäude", "Hochhaus", Option(852)),
					Link("Postbank", "Postbank", Option(925)),
					Link("Hochhaus", "Hochhaus", Option(943)),
					Link("Gebäude", "Hochhaus", Option(1093)),
					Link("Berlin", "Berlin", Option(1131)),
					Link("Berlin", "Berlin", Option(1270)),
					Link("Hochhaus", "Hochhaus", Option(1282)),
					Link("New York", "New York City", Option(1472)),
					Link("Gebäude", "Hochhaus", Option(1489)),
					Link("Gebäude", "Hochhaus", Option(1639)),
					Link("Hochhaus", "Hochhaus", Option(1708)),
					Link("Bronze", "Bronze", Option(1800)),
					Link("Media Broadcast", "Media Broadcast", Option(1906)),
					Link("Deutsche Funkturm", "Deutsche Funkturm", Option(1944)),
					Link("DFMG", "Deutsche Funkturm", Option(1963)),
					Link("Deutschen Telekom AG", "Deutsche Telekom", Option(1999)),
					Link("Berliner", "Berlin", Option(2052))
				)
			)
		)
	}

	def wikidataEntities(): List[WikiDataEntity] = {
		List(
			WikiDataEntity("Q1", instancetype = Option("comp"), wikiname = Option("Page 1")),
			WikiDataEntity("Q2", instancetype = Option("comp"), wikiname = Option("Page 2")),
			WikiDataEntity("Q3", instancetype = Option("comp"), wikiname = Option("Page 3")),
			WikiDataEntity("Q4", instancetype = Option("comp")),
			WikiDataEntity("Q5", wikiname = Option("Page 5")),
			WikiDataEntity("Q6"))
	}

	def wikidataCompanyPages(): List[String] = {
		List("Page 1", "Page 2", "Page 3")
	}

	def companyPages(): List[Page] = {
		List(
			Page("Page 1", Map("P1Alias1" -> 1, "P1Alias2" -> 1)),
			Page("Page 2", Map("P2Alias1" -> 1, "P2Alias2" -> 1)),
			Page("Page 3", Map("P3Alias1" -> 1, "P1Alias1" -> 1)),
			Page("Page 4", Map("P4Alias1" -> 1, "P4Alias3" -> 1)),
			Page("Page 5", Map("P5Alias1" -> 1)),
			Page("Page 6", Map("P6Alias1" -> 1)))
	}

	def companyAliases(): Set[String] = {
		Set("P1Alias1", "P1Alias2", "P2Alias1", "P2Alias2", "P3Alias1")
	}

	def unfilteredCompanyLinksEntries(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				"Title 1",
				textlinks = List(Link("P1Alias1", "Page 1"), Link("P4Alias3", "Page 4")),
				templatelinks = List(Link("P2Alias1", "Page 2")),
				categorylinks = List(Link("P3Alias1", "Page 3")),
				listlinks = List(Link("P4Alias1", "Page 4"))),
			ParsedWikipediaEntry(
				"Title 2",
				textlinks = List(Link("P5Alias1", "Page 5")),
				categorylinks = List(Link("P6Alias1", "Page 6"))),
			ParsedWikipediaEntry(
				"Title 3",
				listlinks = List(Link("P1Alias2", "Page 1")),
				disambiguationlinks = List(Link("P2Alias2", "Page 2")))
		)
	}

	def filteredCompanyLinksEntries(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				"Title 1",
				textlinks = List(Link("P1Alias1", "Page 1")),
				templatelinks = List(Link("P2Alias1", "Page 2")),
				categorylinks = List(Link("P3Alias1", "Page 3"))),
			ParsedWikipediaEntry("Title 2"),
			ParsedWikipediaEntry(
				"Title 3",
				listlinks = List(Link("P1Alias2", "Page 1")),
				disambiguationlinks = List(Link("P2Alias2", "Page 2")))
		)
	}

	def classifierFeatureEntries(): List[FeatureEntry] = {
		List(
			FeatureEntry("alias1", "page1", 0.01, 0.01, 0.1, false),
			FeatureEntry("alias2", "page2", 0.3, 0.7, 0.9, true),
			FeatureEntry("alias3", "page3", 0.1, 0.2, 0.3, false),
			FeatureEntry("alias4", "page4", 0.2, 0.1, 0.2, false),
			FeatureEntry("alias5", "page5", 0.05, 0.6, 0.7, true),
			FeatureEntry("alias6", "page6", 0.03, 0.1, 0.3, false),
			FeatureEntry("alias7", "page7", 0.2, 0.7, 0.6, true))
	}
}

// scalastyle:on method.length
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
// scalastyle:on file.size.limit
