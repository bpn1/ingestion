/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining

import de.hpi.ingestion.dataimport.wikidata.models.WikidataEntity
import de.hpi.ingestion.dataimport.wikipedia.models.WikipediaEntry
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.deduplication.models.{PrecisionRecallDataTuple, SimilarityMeasureStats}
import de.hpi.ingestion.textmining.models.{TrieAlias, _}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node, Predict, RandomForestModel}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, TextNode}
import scala.collection.JavaConversions._
import scala.io.{BufferedSource, Source}
import java.io._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import de.hpi.ingestion.textmining.preprocessing.LocalTrieBuilder

// scalastyle:off number.of.methods
// scalastyle:off line.size.limit
// scalastyle:off method.length
// scalastyle:off file.size.limit

object TestData {

	var rfModel: Option[PipelineModel] = None

	def profilerTfidfVectors(): Vector[SparseVector] = {
		Vector(
			Vectors.sparse(1048576, Array(243877, 626966, 787968), Array(0.0, 0.0, 0.0)),
			Vectors.sparse(1048576, Array(243877, 626966, 787968), Array(0.0, 0.0, 0.0)),
			Vectors.sparse(1048576, Array(160447, 162399, 215849, 237331, 262902, 292496, 350912, 456939, 457955, 460563, 461447, 463664, 482788, 487479, 491236, 512566, 513632, 539064, 569819, 573237, 578289, 584752, 592681, 602984, 611518, 649006, 672104, 787968, 851566, 864188, 878450, 925525, 949220, 980295, 981891, 989779, 997427, 998788, 1002571, 1020442, 1028132), Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)),
			Vectors.sparse(1048576, Array(160447, 215849, 457955, 459965, 461447, 626966, 787968, 806439, 932746, 971734, 997427, 1002571, 1028132), Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
		).map(_.asInstanceOf[SparseVector])
	}

	def profilerTfidfMaps(): Set[(String, Map[String, Double])] = {
		Set (
			("Audi Test mit Link", Map("audi" -> 0.12493873660829993, "verlink" -> 0.0, "." -> 0.0)),
			("Audi Test ohne Link", Map("audi" -> 0.12493873660829993, "verlink" -> 0.0, "." -> 0.0)),
			("Streitberg (Brachttal)", Map("1554" -> 0.0, "waldrech" -> 0.0, "einwohnerzahl" -> 0.0, "streidtburgk" -> 0.0, "19" -> 0.0, "." -> 0.0, "brachttal" -> 0.0, "ort" -> 0.0, "jahrhu" -> 0.0, "nachweislich" -> 0.0, "-lrb-" -> 0.0, "huterech" -> 0.0, "eingeburg" -> 0.0, "steytberg" -> 0.0, "erwahnung" -> 0.0, "ortsteil" -> 0.0, "bezeichnung" -> 0.0, "jahr" -> 0.0, "270" -> 0.0, "-" -> 0.0, "stridberg" -> 0.0, "," -> 0.0, "kleinst" -> 0.0, "-rrb-" -> 0.0, "stamm" -> 0.0, "hess" -> 0.0, "holx" -> 0.0, "buding" -> 0.0, "tauch" -> 0.0, "stripurgk" -> 0.0, "1500" -> 0.0, "gemei" -> 0.0, "1377" -> 0.0, "wald" -> 0.0, "main-kinzig-kreis" -> 0.0, "1528" -> 0.0, "namensvaria" -> 0.0, "ortsnam" -> 0.0, "streitberg" -> 0.0, "mittelal" -> 0.0, "red" -> 0.0)),
			("Testartikel", Map("." -> 0.0, "brachttal" -> 0.0, "jahr" -> 0.0, "audi" -> 0.12493873660829993, "," -> 0.0, "hess" -> 0.0, "buding" -> 0.0, "historisch" -> 0.0, "wald" -> 0.0, "main-kinzig-kreis" -> 0.0, ":" -> 0.0, "nochmal" -> 0.0, "backfisch" -> 0.6020599913279624))
		)
	}

	def profilingStatisticLables(): Set[String] = {
		Set(
			"Spark MLlib Context Extraction",
			"Ingestion Context Extraction",
			"Spark MLlib Tfidf Computation",
			"Ingestion Tfidf Computation",
			"Spark MLlib total",
			"Ingestion total"
		)
	}

	def tokenizedDocumentsList(): List[List[String]] = {
		List(
			List("Hier", "ist", "Audi", "verlinkt."),
			List("Hier", "ist", "Audi", "nicht", "verlinkt.")
		)
	}

	def valuesList(): List[Double] = {
		List(5.0, 7.1, 5.0, 2.9, 7.1)
	}

	def ranksList(): List[Int] = {
		List(3, 1, 3, 5, 1)
	}

	def longValuesList(): List[Double] = {
		List(0.72, 0.038415841584158415, 0.05821782178217822, 0.0004, 0.0004, 0.0004, 0.0004, 0.001188118811881188, 0.0004,
			0.0004, 0.0004, 0.0004, 0.0004, 0.0004, 0.17504950495049504, 0.0004, 0.001188118811881188, 7.920792079207921E-4)
	}

	def longRanksList(): List[Int] = {
		List(1, 4, 3, 8, 8, 8, 8, 5, 8,
			8, 8, 8, 8, 8, 2, 8, 5, 7)
	}

	def deltaTopValuesList(): List[Double] = {
		List(2.0999999999999996, Double.PositiveInfinity, 2.0999999999999996, 4.199999999999999, Double.PositiveInfinity)
	}

	def deltaSuccessorValuesList(): List[Double] = {
		List(2.1, 2.0999999999999996, 2.1, Double.PositiveInfinity, 2.0999999999999996)
	}

	def sentencesList(): List[String] = {
		List(
			"This is a test sentence.",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen.",
			"Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen.",
			"Dieser Satz enthält Klammern (evtl. problematisch)."
		)
	}

	def uncleanTokenizedSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence."),
			List("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde", "Brachttal,", "Main-Kinzig-Kreis", "in", "Hessen."),
			List("Links:", "Audi,", "Brachttal,", "historisches", "Jahr.\nKeine", "Links:", "Hessen,", "Main-Kinzig-Kreis,", "Büdinger", "Wald,", "Backfisch", "und", "nochmal", "Hessen."),
			List("Dieser", "Satz", "enthält", "Klammern", "(evtl.", "problematisch).")
		)
	}

	def tokenizedSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "in", "Hessen"),
			List("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine", "Links", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen"),
			List("Dieser", "Satz", "enthält", "Klammern", "-LRB-", "evtl", "problematisch", "-RRB-")
		)
	}

	def tokenizedSentencesWithoutSpecialCharacters(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "in", "Hessen"),
			List("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine", "Links", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen"),
			List("Dieser", "Satz", "enthält", "Klammern", "evtl", "problematisch")
		)
	}

	def filteredTokenizedSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence"),
			List("Streitberg", "Ortsteilen", "Gemeinde", "Brachttal", "Main-Kinzig-Kreis", "Hessen"),
			List("Audi", "Brachttal", "historisches", "Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "nochmal", "Hessen"),
			List("Satz", "enthält", "Klammern", "-LRB-", "evtl", "problematisch", "-RRB-")
		)
	}

	def filteredUncleanTokenizedSentences(): List[List[String]] = {
		List(
			List("This", "is", "a", "test", "sentence", "."),
			List("Streitberg", "Ortsteilen", "Gemeinde", "Brachttal", ",", "Main-Kinzig-Kreis", "Hessen", "."),
			List(":", "Audi", ",", "Brachttal", ",", "historisches", "Jahr", ".", ":", "Hessen", ",", "Main-Kinzig-Kreis", ",", "Büdinger", "Wald", ",", "Backfisch", "nochmal", "Hessen", "."),
			List("Satz", "enthält", "Klammern", "-LRB-", "evtl", ".", "problematisch", "-RRB-", ".")
		)
	}

	def stemmedTokenizedSentences(): List[List[String]] = {
		List(
			List("thi", "is", "a", "test", "sentenc"),
			List("streitberg", "ist", "ein", "von", "sech", "ortsteil", "der", "gemei", "brachttal", "main-kinzig-kreis", "in", "hess"),
			List("link", "audi", "brachttal", "historisch", "jahr", "kein", "link", "hess", "main-kinzig-kreis", "buding", "wald", "backfisch", "und", "nochmal", "hess"),
			List("dies", "satx", "enthal", "klamm", "-lrb-", "evtl", "problematisch", "-rrb-")
		)
	}

	def stemmedAndFilteredSentences(): List[List[String]] = {
		List(
			List("thi", "is", "a", "test", "sentenc"),
			List("streitberg", "ortsteil", "gemei", "brachttal", "main-kinzig-kreis", "hess"),
			List("audi", "brachttal", "historisch", "jahr", "hess", "main-kinzig-kreis", "buding", "wald", "backfisch", "nochmal", "hess"),
			List("satx", "enthal", "klamm", "-lrb-", "evtl", "problematisch", "-rrb-")
		)
	}

	def stemmedAndFilteredUncleanTokenizedSentences(): List[List[String]] = {
		List(
			List("thi", "is", "a", "test", "sentenc", "."),
			List("streitberg", "ortsteil", "gemei", "brachttal", ",", "main-kinzig-kreis", "hess", "."),
			List(":", "audi", ",", "brachttal", ",", "historisch", "jahr", ".", ":", "hess", ",", "main-kinzig-kreis", ",", "buding", "wald", ",", "backfisch", "nochmal", "hess", "."),
			List("satx", "enthal", "klamm", "-lrb-", "evtl", ".", "problematisch", "-rrb-", ".")
		)
	}

	def reversedSentences(): List[String] = {
		List(
			"This is a test sentence",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal Main-Kinzig-Kreis in Hessen",
			"Links Audi Brachttal historisches Jahr Keine Links Hessen Main-Kinzig-Kreis Büdinger Wald Backfisch und nochmal Hessen",
			"Dieser Satz enthält Klammern -LRB- evtl problematisch -RRB-"
		)
	}

	def cleanedReversedSentences(): List[String] = {
		List(
			"This is a test sentence",
			"Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal Main-Kinzig-Kreis in Hessen",
			"Links Audi Brachttal historisches Jahr Keine Links Hessen Main-Kinzig-Kreis Büdinger Wald Backfisch und nochmal Hessen",
			"Dieser Satz enthält Klammern evtl problematisch"
		)
	}

	def allAliasesSet(): Set[String] = {
		Set(
			"Audi",
			"Brachttal",
			"Main-Kinzig-Kreis",
			"Hessen",
			"1377",
			"Büdinger Wald",
			"Backfisch",
			"Streitberg",
			"historisches Jahr"
		)
	}

	def parsedWikipediaWithTextsSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi Test mit Link",
				Option("Hier ist Audi verlinkt."),
				List(Link("Audi", "Audi", Option(9))),
				List(),
				List("Audi"),
				textlinksreduced = List(Link("Audi", "Audi", Option(9)))),
			ParsedWikipediaEntry(
				"Audi Test ohne Link",
				Option("Hier ist Audi nicht verlinkt."),
				List(),
				List(),
				List("Audi")),
			ParsedWikipediaEntry(
				"Streitberg (Brachttal)",
				Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546))),
				List(),
				List("Streitberg", "Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"),
				textlinksreduced = List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546)))),
			ParsedWikipediaEntry(
				"Testartikel",
				Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24))),
				List(),
				List("Audi", "Brachttal", "historisches Jahr", "Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"),
				textlinksreduced = List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24)))))
	}

	def parsedWikipediaExtendedLinksSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				textlinks = List(Link("Audi", "Audi", Option(9))),
				textlinksreduced = List(Link("Audi", "Audi", Option(9)))
			),
			ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				textlinks = List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66))),
				textlinksreduced = List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66))),
				rawextendedlinks = List(
					ExtendedLink("Hessen", Map("Hessen" -> 10), Option(87)),
					ExtendedLink("1377", Map("1377" -> 10), Option(225)),
					ExtendedLink("Büdinger Wald", Map("Büdinger Wald" -> 10), Option(546)))
			),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				textlinks = List(
					Link("Audi", "Audi", Option(7)),
					Link("historisches Jahr", "1377", Option(24))),
				textlinksreduced = List(
					Link("Audi", "Audi", Option(7)),
					Link("historisches Jahr", "1377", Option(24))),
				rawextendedlinks = List(
					ExtendedLink("Brachttal", Map("Brachttal" -> 10), Option(13)))))
	}

	def articlesWithContextSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi"),
				context = Map("audi" -> 1, "verlink" -> 1),
				textlinksreduced = List(Link("Audi", "Audi", Option(9)))
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
				context = Map("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1),
				textlinksreduced = List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24)))))
	}

	def linksWithContextsSet(): Set[Link] = {
		Set(
			Link("Audi", "Audi", Option(9), Map("verlink" -> 1)),
			Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "stamm" -> 1)),
			Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
			Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "jahr" -> 1)),
			Link("1377", "1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
			Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1)),
			Link("Audi", "Audi", Option(7), Map("brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
			Link("Brachttal", "Brachttal", Option(13), Map("audi" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
			Link("historisches Jahr", "1377", Option(24), Map("audi" -> 1, "brachttal" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
	}

	def articlesWithLinkContextsSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				List(
					Link("Audi", "Audi", Option(9))
				),
				List(),
				List("Audi"),
				linkswithcontext = List(Link("Audi", "Audi", Option(9), Map("verlink" -> 1))),
				textlinksreduced = List(Link("Audi", "Audi", Option(9)))
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
					Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "stamm" -> 1)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
					Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "jahr" -> 1)),
					Link("1377", "1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1))),
				textlinksreduced = List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546)))
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
				textlinksreduced = List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24)))
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
				context = Map("audi" -> 1, "verlink" -> 1),
				textlinksreduced = List(Link("Audi", "Audi", Option(9)))
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
					Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "stamm" -> 1)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
					Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "jahr" -> 1)),
					Link("1377", "1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1))),
				context = Map("1554" -> 1, "waldrech" -> 1, "einwohnerzahl" -> 1, "streidtburgk" -> 1, "19" -> 1, "brachttal" -> 1, "ort" -> 1, "jahrhu" -> 1, "nachweislich" -> 1, "-lrb-" -> 3, "huterech" -> 1, "eingeburg" -> 1, "steytberg" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "-" -> 1, "stridberg" -> 1, "kleinst" -> 1, "-rrb-" -> 3, "stamm" -> 1, "hess" -> 1, "holx" -> 1, "buding" -> 1, "tauch" -> 1, "stripurgk" -> 1, "1500" -> 1, "gemei" -> 1, "1377" -> 1, "wald" -> 1, "main-kinzig-kreis" -> 1, "1528" -> 1, "namensvaria" -> 1, "ortsnam" -> 1, "streitberg" -> 2, "mittelal" -> 1, "red" -> 1, "stamm" -> 1),
				textlinksreduced = List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546)))
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
				context = Map("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1),
				textlinksreduced = List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24)))
			))
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

	def transformedArticles(): Set[(String, Bag[String, Int])] = {
		Set(
			("Audi Test mit Link", Bag("audi" -> 1, "verlink" -> 1)),
			("Audi Test ohne Link", Bag("audi" -> 1, "verlink" -> 1)),
			("Streitberg (Brachttal)", Bag("1554" -> 1, "waldrech" -> 1, "einwohnerzahl" -> 1, "streidtburgk" -> 1, "19" -> 1, "brachttal" -> 1, "ort" -> 1, "jahrhu" -> 1, "nachweislich" -> 1, "-lrb-" -> 3, "huterech" -> 1, "eingeburg" -> 1, "steytberg" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "-" -> 1, "stridberg" -> 1, "kleinst" -> 1, "-rrb-" -> 3, "stamm" -> 1, "hess" -> 1, "holx" -> 1, "buding" -> 1, "tauch" -> 1, "stripurgk" -> 1, "1500" -> 1, "gemei" -> 1, "1377" -> 1, "wald" -> 1, "main-kinzig-kreis" -> 1, "1528" -> 1, "namensvaria" -> 1, "ortsnam" -> 1, "streitberg" -> 2, "mittelal" -> 1, "red" -> 1)),
			("Testartikel", Bag("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
	}

	def transformedLinkContexts(): Set[(Link, Bag[String, Int])] = {
		Set(
			(Link("Audi", "Audi", Option(9), Map(), Option("Audi Test mit Link")), Bag("verlink" -> 1)),
			(Link("Brachttal", "Brachttal", Option(55), Map(), Option("Streitberg (Brachttal)")), Bag("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "stamm" -> 1)),
			(Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map(), Option("Streitberg (Brachttal)")), Bag("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
			(Link("Hessen", "Hessen", Option(87), Map(), Option("Streitberg (Brachttal)")), Bag("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "jahr" -> 1)),
			(Link("1377", "1377", Option(225), Map(), Option("Streitberg (Brachttal)")), Bag("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
			(Link("Büdinger Wald", "Büdinger Wald", Option(546), Map(), Option("Streitberg (Brachttal)")), Bag("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1)),
			(Link("Audi", "Audi", Option(7), Map(), Option("Testartikel")), Bag("brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
			(Link("Brachttal", "Brachttal", Option(13), Map(), Option("Testartikel")), Bag("audi" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
			(Link("historisches Jahr", "1377", Option(24), Map(), Option("Testartikel")), Bag("audi" -> 1, "brachttal" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1))
		)
	}

	def linkContextsTfidfList(): List[(Link, Map[String, Double])] = {
		// retrieved from 4 documents
		val tf1df1 = 0.6020599913279624
		val tf1df2 = 0.3010299956639812
		val tf1df3 = 0.12493873660829993
		val tf2df1 = 1.2041199826559248
		val tf2df2 = 0.6020599913279624
		val tf3df2 = 0.9030899869919435
		List(
			(Link("Audi", "Audi", Option(9), article = Option("Audi Test mit Link")), Map("verlink" -> tf1df2)),
			(Link("Brachttal", "Brachttal", Option(55), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "hess" -> tf1df2, "gemei" -> tf1df1, "main-kinzig-kreis" -> tf1df2, "streitberg" -> tf1df1, "stamm" -> tf2df2)),
			(Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "hess" -> tf1df2, "gemei" -> tf1df1, "streitberg" -> tf1df1)),
			(Link("Hessen", "Hessen", Option(87), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "gemei" -> tf1df1, "main-kinzig-kreis" -> tf1df2, "streitberg" -> tf1df1, "jahr" -> tf1df2)),
			(Link("1377", "1377", Option(225), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "streidtburgk" -> tf1df1, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf1df1, "bezeichnung" -> tf1df1, "jahr" -> tf3df2, "270" -> tf1df1, "stridberg" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "tauch" -> tf1df1, "1500" -> tf1df1, "namensvaria" -> tf1df1, "red" -> tf1df1)),
			(Link("Büdinger Wald", "Büdinger Wald", Option(546), article = Option("Streitberg (Brachttal)")), Map("waldrech" -> tf1df1, "19" -> tf1df1, "ort" -> tf1df1, "jahrhu" -> tf1df1, "-lrb-" -> tf1df1, "huterech" -> tf1df1, "eingeburg" -> tf1df1, "-" -> tf1df1, "-rrb-" -> tf1df1, "holx" -> tf1df1, "ortsnam" -> tf1df1, "streitberg" -> tf1df1, "mittelal" -> tf1df1)),
			(Link("Audi", "Audi", Option(7), article = Option("Testartikel")), Map("brachttal" -> tf1df2, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("Brachttal", "Brachttal", Option(13), article = Option("Testartikel")), Map("audi" -> tf1df3, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("historisches Jahr", "1377", Option(24), article = Option("Testartikel")), Map("audi" -> tf1df3, "brachttal" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1))
		).sortBy(_._1.alias)
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
			(Link("Audi", "Audi", Option(9), article = Option("Audi Test mit Link")), Map("verlink" -> tf1df2)),
			(Link("Brachttal", "Brachttal", Option(55), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "hess" -> tf1df2, "gemei" -> tf1df1, "main-kinzig-kreis" -> tf1df2, "streitberg" -> tf1df1)),
			(Link("Main-Kinzig-Kreis", null, Option(66), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "hess" -> tf1df2, "gemei" -> tf1df1, "streitberg" -> tf1df1)),
			(Link("Hessen", "Hessen", Option(87), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "brachttal" -> tf1df2, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf2df1, "270" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "gemei" -> tf1df1, "main-kinzig-kreis" -> tf1df2, "streitberg" -> tf1df1)),
			(Link("1377", null, Option(225), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "streidtburgk" -> tf1df1, "nachweislich" -> tf1df1, "erwahnung" -> tf1df1, "ortsteil" -> tf1df1, "bezeichnung" -> tf1df1, "jahr" -> tf3df2, "270" -> tf1df1, "stridberg" -> tf1df1, "kleinst" -> tf1df1, "stamm" -> tf1df1, "tauch" -> tf1df1, "1500" -> tf1df1, "namensvaria" -> tf1df1, "red" -> tf1df1)),
			(Link("Büdinger Wald", "Büdinger Wald", Option(546), article = Option("Streitberg (Brachttal)")), Map("waldrech" -> tf1df1, "19" -> tf1df1, "ort" -> tf1df1, "jahrhu" -> tf1df1, "-lrb-" -> tf1df1, "huterech" -> tf1df1, "eingeburg" -> tf1df1, "-" -> tf1df1, "-rrb-" -> tf1df1, "holx" -> tf1df1, "ortsnam" -> tf1df1, "streitberg" -> tf1df1, "mittelal" -> tf1df1)),
			(Link("Audi", "Audi", Option(7), article = Option("Testartikel")), Map("brachttal" -> tf1df2, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("Brachttal", null, Option(13), article = Option("Testartikel")), Map("audi" -> tf1df3, "historisch" -> tf1df1, "jahr" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)),
			(Link("historisches Jahr", "1377", Option(24), article = Option("Testartikel")), Map("audi" -> tf1df3, "brachttal" -> tf1df2, "hess" -> tf2df2, "main-kinzig-kreis" -> tf1df2, "buding" -> tf1df2, "wald" -> tf1df2, "backfisch" -> tf1df1, "nochmal" -> tf1df1)))
	}

	def termFrequenciesSet(): Set[(String, Bag[String, Int])] = {
		Set(
			("Audi Test mit Link", Bag("audi" -> 1, "verlink" -> 1)),
			("Audi Test ohne Link", Bag("audi" -> 1, "verlink" -> 1)),
			("Testartikel", Bag("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
	}

	def aliasOccurrencesInArticlesList(): List[AliasOccurrencesInArticle] = {
		List(
			AliasOccurrencesInArticle(Set("Audi"), Set()),
			AliasOccurrencesInArticle(Set(), Set("Audi")),
			AliasOccurrencesInArticle(Set("Brachttal", "Main-Kinzig-Kreis", "Hessen", "1377", "Büdinger Wald"), Set("Streitberg")),
			AliasOccurrencesInArticle(Set("Audi", "Brachttal", "historisches Jahr"), Set("Hessen", "Main-Kinzig-Kreis", "Büdinger Wald", "Backfisch"))
		)
	}

	def startAliasCounterList(): List[Alias] = {
		List(
			Alias("Audi", Map(), Map(), Option(1), Option(1)),
			Alias("Audi", Map(), Map(), Option(1), Option(1)),
			Alias("Audi", Map(), Map(), Option(0), Option(1)),
			Alias("Brachttal", Map(), Map(), Option(1), Option(1)),
			Alias("Brachttal", Map(), Map(), Option(1), Option(1)),
			Alias("Main-Kinzig-Kreis", Map(), Map(), Option(1), Option(1)),
			Alias("Main-Kinzig-Kreis", Map(), Map(), Option(0), Option(1)),
			Alias("Hessen", Map(), Map(), Option(1), Option(1)),
			Alias("Hessen", Map(), Map(), Option(0), Option(1)),
			Alias("1377", Map(), Map(), Option(1), Option(1)),
			Alias("Büdinger Wald", Map(), Map(), Option(1), Option(1)),
			Alias("Büdinger Wald", Map(), Map(), Option(0), Option(1)),
			Alias("Backfisch", Map(), Map(), Option(0), Option(1)),
			Alias("Streitberg", Map(), Map(), Option(0), Option(1)),
			Alias("historisches Jahr", Map(), Map(), Option(1), Option(1))
		)
	}

	def countedAliasesSet(): Set[Alias] = {
		Set(
			Alias("Audi", Map(), Map(), Option(2), Option(3)),
			Alias("Brachttal", Map(), Map(), Option(2), Option(2)),
			Alias("Main-Kinzig-Kreis", Map(), Map(), Option(1), Option(2)),
			Alias("Hessen", Map(), Map(), Option(1), Option(2)),
			Alias("1377", Map(), Map(), Option(1), Option(1)),
			Alias("Büdinger Wald", Map(), Map(), Option(1), Option(2)),
			Alias("Backfisch", Map(), Map(), Option(0), Option(1)),
			Alias("Streitberg", Map(), Map(), Option(0), Option(1)),
			Alias("historisches Jahr", Map(), Map(), Option(1), Option(1))
		)
	}

	def linksSet(): Set[Alias] = {
		Set(
			Alias("Audi", Map("Audi" -> 2)),
			Alias("Brachttal", Map("Brachttal" -> 2)),
			Alias("Main-Kinzig-Kreis", Map("Main-Kinzig-Kreis" -> 1)),
			Alias("Hessen", Map("Hessen" -> 1)),
			Alias("1377", Map("1377" -> 1)),
			Alias("Büdinger Wald", Map("Büdinger Wald" -> 1)),
			Alias("historisches Jahr", Map("1377" -> 1))
		)
	}

	def aliasCountsSet(): Set[(String, Option[Int], Option[Int])] = {
		Set(
			("Audi", Option(2), Option(3)),
			("Brachttal", Option(2), Option(2)),
			("Main-Kinzig-Kreis", Option(1), Option(2)),
			("Hessen", Option(1), Option(2)),
			("1377", Option(1), Option(1)),
			("Büdinger Wald", Option(1), Option(2)),
			("Backfisch", Option(0), Option(1)),
			("Streitberg", Option(0), Option(1)),
			("historisches Jahr", Option(1), Option(1))
		)
	}

	def finalAliasesSet(): Set[Alias] = {
		Set(
			Alias("Audi", Map("Audi" -> 2), linkoccurrences = Option(2), totaloccurrences = Option(3)),
			Alias("Brachttal", Map("Brachttal" -> 2), linkoccurrences = Option(2), totaloccurrences = Option(2)),
			Alias("Main-Kinzig-Kreis", Map("Main-Kinzig-Kreis" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(2)),
			Alias("Hessen", Map("Hessen" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(2)),
			Alias("1377", Map("1377" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(1)),
			Alias("Büdinger Wald", Map("Büdinger Wald" -> 4, "Audi" -> 1), linkoccurrences = Option(5), totaloccurrences = Option(10)), // negative example
			Alias("historisches Jahr", Map("1377" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(1)),
			Alias("historisches Jahr", Map("1377" -> 1)) // incomplete table entry
		)
	}

	def aliasesWithExistingPagesSet(): Set[Alias] = {
		// These pages do not fit to the aliases but are required for the tests.
		Set(
			Alias("Audi", Map("Audi Test mit Link" -> 2), linkoccurrences = Option(2), totaloccurrences = Option(3)),
			Alias("Brachttal", Map("Audi Test ohne Link" -> 2), linkoccurrences = Option(2), totaloccurrences = Option(2)),
			Alias("Main-Kinzig-Kreis", Map("Streitberg (Brachttal)" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(2)),
			Alias("Hessen", Map("Streitberg (Brachttal)" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(2)),
			Alias("1377", Map("Testartikel" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(1)),
			Alias("Büdinger Wald", Map("Testartikel" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(2)),
			Alias("historisches Jahr", Map("Testartikel" -> 1), linkoccurrences = Option(1), totaloccurrences = Option(1))
		)
	}

	def existingPagesTfIdfMap(): List[ArticleTfIdf] = {
		List(
			ArticleTfIdf("Audi Test mit Link", Map()),
			ArticleTfIdf("Audi Test ohne Link", Map()),
			ArticleTfIdf("Streitberg (Brachttal)", Map()),
			ArticleTfIdf("Testartikel", Map())
		)
	}

	def allPageNamesOfTestArticleList(): Set[String] = {
		Set(
			"Audi",
			"Brachttal",
			"1377"
		)
	}

	def articleContextWordSets(): Map[String, Set[String]] = {
		Map(
			"Testartikel" -> Set("Links", "Audi", "Brachttal", "historisches", "Jahr", "Keine",
				"Main-Kinzig-Kreis", "Büdinger", "Wald", "Backfisch", "und", "nochmal", "Hessen"),
			"Streitberg (Brachttal)" ->
				Set("Streitberg", "ist", "einer", "von", "sechs", "Ortsteilen", "der", "Gemeinde",
					"Im", "Jahre", "1500", "ist", "von", "Stridberg", "die", "Vom", "Mittelalter",
					"bis", "ins", "Jahrhundert", "hatte", "der", "Ort", "Waldrechte")
		)
	}

	def documentFrequenciesSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("backfisch", 1)
		)
	}

	def filteredDocumentFrequenciesSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 3)
		)
	}

	def filteredDocumentFrequenciesWithSymbols(): List[DocumentFrequency] = {
		List(
			DocumentFrequency("audi", 3),
			DocumentFrequency(".", 4))
	}

	def requestedDocumentFrequenciesSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("backfisch", 2)
		)
	}

	def unstemmedDocumentFrequenciesSet(): Set[DocumentFrequency] = {
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

	def incorrectStemmedDocumentFrequenciesSet(): Set[DocumentFrequency] = {
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

	def stemmedDocumentFrequenciesSet(): Set[DocumentFrequency] = {
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
			DocumentFrequency("nochmal", 1)
		)
	}

	def filteredStemmedDocumentFrequenciesSet(): Set[DocumentFrequency] = {
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
			DocumentFrequency("und", 2)
		)
	}

	def filteredStemmedDocumentFrequenciesSmallSet(): Set[DocumentFrequency] = {
		// without article about Streitburg
		Set(
			DocumentFrequency("audi", 3),
			DocumentFrequency("ist", 2),
			DocumentFrequency("hier", 2),
			DocumentFrequency("verlink", 2)
		)
	}

	def filteredStemmedIdfMap(): Map[String, Double] = {
		val numDoc = 4.0
		Map(
			"ist" -> 3.0 / numDoc,
			"audi" -> 3.0 / numDoc,
			"hier" -> 2.0 / numDoc,
			"verlink" -> 2.0 / numDoc,
			"brachttal" -> 2.0 / numDoc,
			"main-kinzig-kreis" -> 2.0 / numDoc,
			"hess" -> 2.0 / numDoc,
			"buding" -> 2.0 / numDoc,
			"wald" -> 2.0 / numDoc,
			"jahr" -> 2.0 / numDoc,
			"und" -> 2.0 / numDoc)
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

	def articleWordsTfidfMap(): Map[String, Map[String, Double]] = {
		// considered to be retrieved from 7 documents
		val tf1df1 = 0.845
		val tf1df2 = 0.544
		val tf1df3 = 0.368
		Map(
			"Audi" -> Map("audi" -> tf1df1, "ist" -> tf1df3, "ein" -> tf1df3, "automobilhersteller" -> tf1df1),
			"Brachttal" -> Map("brachttal" -> tf1df1, "ist" -> tf1df3, "irgendein" -> tf1df1, "ortsteil" -> tf1df1),
			"Main-Kinzig-Kreis" -> Map("der" -> tf1df2, "main-kinzig-kreis" -> tf1df2, "ein" -> tf1df3, "einwohnerzahl" -> tf1df1),
			"Hessen" -> Map("hessen" -> tf1df1, "ist" -> tf1df3, "der" -> tf1df2, "main-kinzig-kreis" -> tf1df2),
			"1377" -> Map("ein" -> tf1df3, "jahr" -> tf1df2),
			"Büdinger Wald" -> Map("waldrech" -> tf1df1),
			"historisches Jahr" -> Map("jahr" -> tf1df2)
		)
	}

	def articleTfidfList(): List[ArticleTfIdf] = {
		// considered to be retrieved from 7 documents
		val tf1df1 = 0.845
		val tf1df2 = 0.544
		val tf1df3 = 0.368
		List(
			ArticleTfIdf("Audi", Map("audi" -> tf1df1, "ist" -> tf1df3, "ein" -> tf1df3, "automobilhersteller" -> tf1df1)),
			ArticleTfIdf("Brachttal", Map("brachttal" -> tf1df1, "ist" -> tf1df3, "irgendein" -> tf1df1, "ortsteil" -> tf1df1)),
			ArticleTfIdf("Main-Kinzig-Kreis", Map("der" -> tf1df2, "main-kinzig-kreis" -> tf1df2, "ein" -> tf1df3, "einwohnerzahl" -> tf1df1)),
			ArticleTfIdf("Hessen", Map("hessen" -> tf1df1, "ist" -> tf1df3, "der" -> tf1df2, "main-kinzig-kreis" -> tf1df2)),
			ArticleTfIdf("1377", Map("ein" -> tf1df3, "jahr" -> tf1df2)),
			ArticleTfIdf("Büdinger Wald", Map("waldrech" -> tf1df1)),
			ArticleTfIdf("historisches Jahr", Map("jahr" -> tf1df2))
		)
	}

	def shortLinkContextsTfidfList(): List[(Link, Map[String, Double])] = {
		// considered to be retrieved from 7 documents
		val tf1df1 = 0.845
		val tf1df2 = 0.544
		val tf1df3 = 0.368
		List(
			(Link("Audi", "Audi", Option(9), article = Option("Autohersteller")), Map("hier" -> tf1df2, "ist" -> tf1df3, "automobilhersteller" -> tf1df1)),
			(Link("Audi", "Audi", Option(9), article = Option("Autohersteller")), Map("audi" -> tf1df2, "ein" -> tf1df3)),
			(Link("Brachttal", "Brachttal", Option(55), article = Option("Tal")), Map("ist" -> tf1df3, "ortsteil" -> tf1df1)),
			(Link("Brachttal", "Brachttal", Option(13), article = Option("Tal")), Map("tal" -> tf1df1)),
			(Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), article = Option("Tal")), Map("einwohnerzahl" -> tf1df1, "ein" -> tf1df3, "hessen" -> tf1df2)),
			(Link("Hessen", "Hessen", Option(87), article = Option("Tal")), Map("main-kinzig-kreis" -> tf1df2)),
			(Link("1377", "1377", Option(225), article = Option("Reiseziel")), Map("jahr" -> tf1df2)),
			(Link("Büdinger Wald", "Büdinger Wald", Option(546), article = Option("Reiseziel")), Map("waldrech" -> tf1df1)),
			(Link("historisches Jahr", "1377", Option(24), article = Option("Reiseziel")), Map("jahr" -> tf1df2))
		)
	}

	def deadAliasContextsTfidfList(): List[(Link, Map[String, Double])] = {
		List(
			(Link("dead alias 1", "1377", Option(24)), Map("jahr" -> 0.1)),
			(Link("dead alias 2", "1377", Option(24)), Map("jahr" -> 0.2)),
			(Link("dead alias 3", "1377", Option(24)), Map("jahr" -> 0.3))
		)
	}

	def shortLinkAndAliasContextsTfidfList(): List[(Link, Map[String, Double])] = {
		// considered to be retrieved from 7 documents
		val tf1df1 = 0.845
		val tf1df2 = 0.544
		val tf1df3 = 0.368
		List(
			(Link("Audi", "Audi", Option(9), article = Option("Audi Test mit Link")), Map("hier" -> tf1df2, "ist" -> tf1df3, "automobilhersteller" -> tf1df1)),
			(Link("Audi", "Audi", Option(7), article = Option("Testartikel")), Map("audi" -> tf1df2, "ein" -> tf1df3)),
			(Link("Brachttal", "Brachttal", Option(55), article = Option("Streitberg (Brachttal)")), Map("ist" -> tf1df3, "ortsteil" -> tf1df1)),
			(Link("Brachttal", null, Option(13), article = Option("Testartikel")), Map("tal" -> tf1df1)),
			(Link("Main-Kinzig-Kreis", null, Option(66), article = Option("Streitberg (Brachttal)")), Map("einwohnerzahl" -> tf1df1, "ein" -> tf1df3, "hessen" -> tf1df2)),
			(Link("Hessen", "Hessen", Option(87), article = Option("Streitberg (Brachttal)")), Map("main-kinzig-kreis" -> tf1df2)),
			(Link("1377", null, Option(225), article = Option("Streitberg (Brachttal)")), Map("jahr" -> tf1df2)),
			(Link("Büdinger Wald", "Büdinger Wald", Option(546), article = Option("Streitberg (Brachttal)")), Map("waldrech" -> tf1df1)),
			(Link("historisches Jahr", "1377", Option(24), article = Option("Testartikel")), Map("jahr" -> tf1df2))
		)
	}

	def aliasPagesScoresMap(): Map[String, List[(String, Double, Double)]] = {
		Map(
			"Audi" -> List(("Audi", 0.6666666666666666, 1.0)),
			"Brachttal" -> List(("Brachttal", 1.0, 1.0)),
			"Main-Kinzig-Kreis" -> List(("Main-Kinzig-Kreis", 0.5, 1.0)),
			"Hessen" -> List(("Hessen", 0.5, 1.0)),
			"1377" -> List(("1377", 1.0, 1.0)),
			"Büdinger Wald" -> List(("Büdinger Wald", 0.5, 0.8), ("Audi", 0.5, 0.2)),
			"historisches Jahr" -> List(("1377", 1.0, 1.0))
		)
	}

	def singleAliasLinkList(): List[(Link, Map[String, Double])] = {
		List(
			(Link("BMW", "BMW", Option(0), article = Option("Automobilhersteller")), Map()),
			(Link("BMW", "BMW-Motorrad", Option(10), article = Option("Automobilhersteller")), Map()),
			(Link("BMW", "BMW (Motorsport)", Option(20), article = Option("Audi")), Map()),
			(Link("BMW", "BMW M10", Option(30), article = Option("Audi")), Map())
		)
	}

	def singleAliasPageScoresMap(): Map[String, List[(String, Double, Double)]] = {
		Map(
			"BMW" -> List(
				("BMW", 0.5962219598583235, 0.7239603960396039),
				("BMW-Motorrad", 0.5962219598583235, 0.17504950495049504),
				("BMW (Motorsport)", 0.5962219598583235, 0.05821782178217822),
				("BMW M10", 0.5962219598583235, 3.9603960396039607E-4))
		)
	}

	def singleAliasManyPagesScoresMap(): Map[String, List[(String, Double, Double)]] = {
		Map(
			"BMW" -> List(
				("BMW", 0.5962219598583235, 0.7239603960396039),
				("BMW-Motorrad", 0.5962219598583235, 0.17504950495049504),
				("BMW (Motorsport)", 0.5962219598583235, 0.05821782178217822),
				("BMW (Automarke)", 0.5962219598583235, 0.038415841584158415),
				("Berlins Most Wanted", 0.5962219598583235, 0.001188118811881188),
				("BMW E36", 0.5962219598583235, 0.001188118811881188),
				("Liste der BMW-Motorräder", 0.5962219598583235, 7.920792079207921E-4),
				("BMW E21", 0.5962219598583235, 3.9603960396039607E-4),
				("BMW M10", 0.5962219598583235, 3.9603960396039607E-4),
				("BMW 501/502", 0.5962219598583235, 3.9603960396039607E-4)
			))
	}

	def aliasWithManyPages(): Alias = {
		Alias("BMW", Map(
			"BMW" -> 1828,
			"BMW-Motorrad" -> 442,
			"BMW (Motorsport)" -> 147,
			"BMW (Automarke)" -> 97,
			"Berlins Most Wanted" -> 3,
			"BMW E36" -> 3,
			"Liste der BMW-Motorräder" -> 2,
			"BMW E21" -> 1,
			"BMW M10" -> 1,
			"BMW 501/502" -> 1
		), linkoccurrences = Option(2525), totaloccurrences = Option(4235))
	}

	def aliasScoresWithManyPages(): Map[String, List[(String, Double, Double)]] = {
		Map(
			"BMW" -> List(
				("BMW-Motorrad", 0.5962219598583235, 0.17504950495049504),
				("BMW", 0.5962219598583235, 0.7239603960396039),
				("BMW (Automarke)", 0.5962219598583235, 0.038415841584158415),
				("Berlins Most Wanted", 0.5962219598583235, 0.001188118811881188),
				("Liste der BMW-Motorräder", 0.5962219598583235,7.920792079207921E-4),
				("BMW 501/502", 0.5962219598583235,3.9603960396039607E-4),
				("BMW M10", 0.5962219598583235,3.9603960396039607E-4),
				("BMW (Motorsport)", 0.5962219598583235, 0.05821782178217822),
				("BMW E21", 0.5962219598583235,3.9603960396039607E-4),
				("BMW E36", 0.5962219598583235, 0.001188118811881188))
		)
	}

	def articlesForSingleAlias(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Automobilhersteller", linkswithcontext = List(Link("BMW", "BMW", Option(0), Map()))),
			ParsedWikipediaEntry("BMW"),
			ParsedWikipediaEntry("BMW-Motorrad"),
			ParsedWikipediaEntry("BMW (Motorsport)"),
			ParsedWikipediaEntry("BMW (Automarke)"),
			ParsedWikipediaEntry("Berlins Most Wanted"),
			ParsedWikipediaEntry("BMW E36"),
			ParsedWikipediaEntry("Liste der BMW-Motorräder"),
			ParsedWikipediaEntry("BMW E21"),
			ParsedWikipediaEntry("BMW M10"),
			ParsedWikipediaEntry("BMW 501/502")
		)
	}

	def emptyArticlesTfidfMap(): Map[String, Map[String, Double]] = {
		Map(
			"BMW" -> Map(),
			"BMW-Motorrad" -> Map(),
			"BMW (Motorsport)" -> Map(),
			"BMW (Automarke)" -> Map(),
			"Berlins Most Wanted" -> Map(),
			"BMW E36" -> Map(),
			"Liste der BMW-Motorräder" -> Map(),
			"BMW E21" -> Map(),
			"BMW M10" -> Map(),
			"BMW 501/502" -> Map()
		)
	}

	def emptyArticlesTfIdfList(): List[ArticleTfIdf] = {
		List(
			ArticleTfIdf("BMW", Map()),
			ArticleTfIdf("BMW-Motorrad", Map()),
			ArticleTfIdf("BMW (Motorsport)", Map()),
			ArticleTfIdf("BMW (Automarke)", Map()),
			ArticleTfIdf("Berlins Most Wanted", Map()),
			ArticleTfIdf("BMW E36", Map()),
			ArticleTfIdf("Liste der BMW-Motorräder", Map()),
			ArticleTfIdf("BMW E21", Map()),
			ArticleTfIdf("BMW M10", Map()),
			ArticleTfIdf("BMW 501/502", Map())
		)
	}

	def featureEntriesForSingleAliasList(): List[FeatureEntry] = {
		val link_score = 0.5962219598583235
		val mfPage_072 = MultiFeature(0.7239603960396039)
		val mfPage_018 = MultiFeature(0.17504950495049504)
		val mfPage_0058 = MultiFeature(0.05821782178217822)
		val mfPage_00004 = MultiFeature(3.9603960396039607E-4)
		val mfCos = MultiFeature(0.0)

		List(
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW", link_score, mfPage_072, mfCos, true),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW", link_score, mfPage_072, mfCos, false),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, true),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Audi", 20, "BMW", "BMW", link_score, mfPage_072, mfCos, false),
			FeatureEntry("Audi", 20, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Audi", 20, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, true),
			FeatureEntry("Audi", 20, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW", link_score, mfPage_072, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, true)
		).sortBy(featureEntry => (featureEntry.article, featureEntry.offset, featureEntry.entity))
	}

	def featureEntriesForSingleAliasWithSOFList(): List[FeatureEntry] = {
		val default = Double.PositiveInfinity
		val link_score = 0.5962219598583235
		val mfPage_072 = MultiFeature(0.7239603960396039, 1, default, 0.548910891089109)
		val mfPage_018 = MultiFeature(0.17504950495049504, 2, 0.548910891089109, 0.11683168316831682)
		val mfPage_0058 = MultiFeature(0.05821782178217822, 3, 0.6657425742574257, 0.057821782178217825)
		val mfPage_00004 = MultiFeature(3.9603960396039607E-4, 4, 0.7235643564356435, default)
		val mfCos = MultiFeature(0.0, 1, default, default)

		List(
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW", link_score, mfPage_072, mfCos, true),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW", link_score, mfPage_072, mfCos, false),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, true),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Automobilhersteller", 10, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Audi", 20, "BMW", "BMW", link_score, mfPage_072, mfCos, false),
			FeatureEntry("Audi", 20, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Audi", 20, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, true),
			FeatureEntry("Audi", 20, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW", link_score, mfPage_072, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Audi", 30, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, true)
		).sortBy(featureEntry => (featureEntry.article, featureEntry.offset, featureEntry.entity))
	}

	def featureEntriesForManyPossibleEntitiesList(): List[FeatureEntry] = {
		val link_score = 0.5962219598583235
		val mfPage_072 = MultiFeature(0.7239603960396039)
		val mfPage_018 = MultiFeature(0.17504950495049504)
		val mfPage_0058 = MultiFeature(0.05821782178217822)
		val mfPage_0038 = MultiFeature(0.038415841584158415)
		val mfPage_00012 = MultiFeature(0.001188118811881188)
		val mfPage_00008 = MultiFeature(7.920792079207921E-4)
		val mfPage_00004 = MultiFeature(3.9603960396039607E-4)
		val mfCos = MultiFeature(0.0)

		List(
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW", link_score, mfPage_072, mfCos, true),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW (Automarke)", link_score, mfPage_0038, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "Berlins Most Wanted", link_score, mfPage_00012, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW E36", link_score, mfPage_00012, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "Liste der BMW-Motorräder", link_score, mfPage_00008, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW E21", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW 501/502", link_score, mfPage_00004, mfCos, false)
		).sortBy(featureEntry => (featureEntry.entity_score.rank, featureEntry.entity))
	}

	def featureEntriesForManyPossibleEntitiesWithSOFList(): List[FeatureEntry] = {
		val default = Double.PositiveInfinity
		val link_score = 0.5962219598583235
		val mfPage_072 = MultiFeature(0.7239603960396039, 1, default, 0.548910891089109)
		val mfPage_018 = MultiFeature(0.17504950495049504, 2, 0.548910891089109, 0.11683168316831682)
		val mfPage_0058 = MultiFeature(0.05821782178217822, 3, 0.6657425742574257, 0.019801980198019806)
		val mfPage_0038 = MultiFeature(0.038415841584158415, 4, 0.6855445544554455, 0.037227722772277226)
		val mfPage_00012 = MultiFeature(0.001188118811881188, 5, 0.7227722772277227, 3.9603960396039596E-4)
		val mfPage_00008 = MultiFeature(7.920792079207921E-4, 7, 0.7231683168316831, 3.9603960396039607E-4)
		val mfPage_00004 = MultiFeature(3.9603960396039607E-4, 8, 0.7235643564356435, default)
		val mfCos = MultiFeature(0.0, 1, default, default)

		List(
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW", link_score, mfPage_072, mfCos, true),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW-Motorrad", link_score, mfPage_018, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW (Motorsport)", link_score, mfPage_0058, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW (Automarke)", link_score, mfPage_0038, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "Berlins Most Wanted", link_score, mfPage_00012, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW E36", link_score, mfPage_00012, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "Liste der BMW-Motorräder", link_score, mfPage_00008, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW E21", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW M10", link_score, mfPage_00004, mfCos, false),
			FeatureEntry("Automobilhersteller", 0, "BMW", "BMW 501/502", link_score, mfPage_00004, mfCos, false)
		).sortBy(featureEntry => (featureEntry.entity_score.rank, featureEntry.entity))
	}

	def featureEntriesList(): List[FeatureEntry] = {
		val mfPage_1 = MultiFeature(1.0)
		val mfPageBüdingerWald_08 = MultiFeature(0.8)
		val mfPageBüdingerWald_02 = MultiFeature(0.2)

		val mfCosAudi_0695 = MultiFeature(0.6951672143063198)
		val mfCosAudi_0608 = MultiFeature(0.608944778982726)
		val mfCosBrachttal_0610 = MultiFeature(0.6107163643669525)
		val mfCosBrachttal_0 = MultiFeature(0.0)
		val mfCosBüdingerWald_1 = MultiFeature(1.0)
		val mfCosBüdingerWald_0 = MultiFeature(0.0)
		val mfCos_0661 = MultiFeature(0.6611213869640233)
		val mfCos_0453 = MultiFeature(0.4531255411026077)
		val mfCos_0828 = MultiFeature(0.828283413127963)

		List(
			FeatureEntry("Autohersteller", 9, "Audi", "Audi", 0.6666666666666666, mfPage_1, mfCosAudi_0608, true),
			FeatureEntry("Autohersteller", 9, "Audi", "Audi", 0.6666666666666666, mfPage_1, mfCosAudi_0695, true),
			FeatureEntry("Tal", 55, "Brachttal", "Brachttal", 1.0, mfPage_1, mfCosBrachttal_0610, true),
			FeatureEntry("Tal", 13, "Brachttal", "Brachttal", 1.0, mfPage_1, mfCosBrachttal_0, true),
			FeatureEntry("Reiseziel", 546, "Büdinger Wald", "Büdinger Wald", 0.5, mfPageBüdingerWald_08, mfCosBüdingerWald_1, true),
			FeatureEntry("Reiseziel", 546, "Büdinger Wald", "Audi", 0.5, mfPageBüdingerWald_02, mfCosBüdingerWald_0, false),
			FeatureEntry("Tal", 66, "Main-Kinzig-Kreis", "Main-Kinzig-Kreis", 0.5, mfPage_1, mfCos_0661, true),
			FeatureEntry("Tal", 87, "Hessen", "Hessen", 0.5, mfPage_1, mfCos_0453, true),
			FeatureEntry("Reiseziel", 225, "1377", "1377", 1.0, mfPage_1, mfCos_0828, true),
			FeatureEntry("Reiseziel", 24, "historisches Jahr", "1377", 1.0, mfPage_1, mfCos_0828, true)
		).sortBy(featureEntry => (featureEntry.article, featureEntry.offset, featureEntry.entity))
	}

	def featureEntriesWitSOFSet(): Set[FeatureEntry] = {
		val default = Double.PositiveInfinity

		val mfPage_1 = MultiFeature(1.0, 1, default, default)
		val mfPageBüdingerWald_08 = MultiFeature(0.8, 1, default, 0.6000000000000001)
		val mfPageBüdingerWald_02 = MultiFeature(0.2, 2, 0.6000000000000001, default)

		val mfCosAudi_0695 = MultiFeature(0.6951672143063198, 1, default, 0.08622243532359375)
		val mfCosAudi_0608 = MultiFeature(0.608944778982726, 2, 0.08622243532359375, default)
		val mfCosBrachttal_0610 = MultiFeature(0.6107163643669525, 1, default, default)
		val mfCosBrachttal_0 = MultiFeature(0.0, 1, default, default)
		val mfCosBüdingerWald_1 = MultiFeature(1.0, 1, default, 1.0)
		val mfCosBüdingerWald_0 = MultiFeature(0.0, 2, 1.0, default)
		val mfCos_0661 = MultiFeature(0.6611213869640233, 1, default, default)
		val mfCos_0453 = MultiFeature(0.4531255411026077, 1, default, default)
		val mfCos_0828 = MultiFeature(0.828283413127963, 1, default, default)

		Set(
			FeatureEntry("Autohersteller", 9, "Audi", "Audi", 0.6666666666666666, mfPage_1, mfCosAudi_0608, true),
			FeatureEntry("Autohersteller", 9, "Audi", "Audi", 0.6666666666666666, mfPage_1, mfCosAudi_0695, true),
			FeatureEntry("Tal", 55, "Brachttal", "Brachttal", 1.0, mfPage_1, mfCosBrachttal_0610, true),
			FeatureEntry("Tal", 13, "Brachttal", "Brachttal", 1.0, mfPage_1, mfCosBrachttal_0, true),
			FeatureEntry("Reiseziel", 546, "Büdinger Wald", "Büdinger Wald", 0.5, mfPageBüdingerWald_08, mfCosBüdingerWald_1, true),
			FeatureEntry("Reiseziel", 546, "Büdinger Wald", "Audi", 0.5, mfPageBüdingerWald_02, mfCosBüdingerWald_0, false),
			FeatureEntry("Tal", 66, "Main-Kinzig-Kreis", "Main-Kinzig-Kreis", 0.5, mfPage_1, mfCos_0661, true),
			FeatureEntry("Tal", 87, "Hessen", "Hessen", 0.5, mfPage_1, mfCos_0453, true),
			FeatureEntry("Reiseziel", 225, "1377", "1377", 1.0, mfPage_1, mfCos_0828, true),
			FeatureEntry("Reiseziel", 24, "historisches Jahr", "1377", 1.0, mfPage_1, mfCos_0828, true)
		)
	}

	def featureEntriesWithAliasesSet(): Set[FeatureEntry] = {
		val mfPage_1 = MultiFeature(1.0)
		val mfPageBüdingerWald_08 = MultiFeature(0.8)
		val mfPageBüdingerWald_02 = MultiFeature(0.2)

		val mfCosAudi_0695 = MultiFeature(0.6951672143063198)
		val mfCosAudi_0608 = MultiFeature(0.608944778982726)
		val mfCosBrachttal_0610 = MultiFeature(0.6107163643669525)
		val mfCosBrachttal_0 = MultiFeature(0.0)
		val mfCosBüdingerWald_1 = MultiFeature(1.0)
		val mfCosBüdingerWald_0 = MultiFeature(0.0)
		val mfCos_0661 = MultiFeature(0.6611213869640233)
		val mfCos_0453 = MultiFeature(0.4531255411026077)
		val mfCos_0828 = MultiFeature(0.828283413127963)

		Set(
			FeatureEntry("Audi Test mit Link", 9, "Audi", "Audi", 0.6666666666666666, mfPage_1, mfCosAudi_0608, true),
			FeatureEntry("Testartikel", 7, "Audi", "Audi", 0.6666666666666666, mfPage_1, mfCosAudi_0695, true),
			FeatureEntry("Streitberg (Brachttal)", 55, "Brachttal", "Brachttal", 1.0, mfPage_1, mfCosBrachttal_0610, true),
			FeatureEntry("Testartikel", 13, "Brachttal", "Brachttal", 1.0, mfPage_1, mfCosBrachttal_0, false),
			FeatureEntry("Streitberg (Brachttal)", 546, "Büdinger Wald", "Büdinger Wald", 0.5, mfPageBüdingerWald_08, mfCosBüdingerWald_1, true),
			FeatureEntry("Streitberg (Brachttal)", 546, "Büdinger Wald", "Audi", 0.5, mfPageBüdingerWald_02, mfCosBüdingerWald_0, false),
			FeatureEntry("Streitberg (Brachttal)", 66, "Main-Kinzig-Kreis", "Main-Kinzig-Kreis", 0.5, mfPage_1, mfCos_0661, false),
			FeatureEntry("Streitberg (Brachttal)", 87, "Hessen", "Hessen", 0.5, mfPage_1, mfCos_0453, true),
			FeatureEntry("Streitberg (Brachttal)", 225, "1377", "1377", 1.0, mfPage_1, mfCos_0828, false),
			FeatureEntry("Testartikel", 24, "historisches Jahr", "1377", 1.0, mfPage_1, mfCos_0828, true)
		)
	}

	def labeledPoints(): List[LabeledPoint] = {
		List(
			LabeledPoint(1.0, new DenseVector(Array(0.1, 0.2, 0.8))),
			LabeledPoint(0.0, new DenseVector(Array(0.4, 0.0, 0.7))),
			LabeledPoint(1.0, new DenseVector(Array(0.7, 1.0, 0.1))),
			LabeledPoint(0.0, new DenseVector(Array(0.8, 0.2, 0.3))),
			LabeledPoint(0.0, new DenseVector(Array(0.9, 0.4, 0.7)))
		)
	}

	def unstemmedGermanWordsSet(): Set[String] = {
		Set("Es", "Keine", "Kein", "Keiner", "Ist", "keine", "keiner", "brauchen", "können", "sollte")
	}

	def wikipediaTextLinks(): Map[String, List[Link]] = {
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
				Link("Leadinstrument", "Lead (Musik)", Option(9))) // Redirect link
		)
	}

	def wikipediaTemplateLinks(): Map[String, List[Link]] = {
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

	def wikipediaTemplateArticles(): Set[String] = {
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
				textlinks = List(Link("Salzachtal", "Salzach#Salzachtal", Option(9))),
				categorylinks = List(Link("Tal im Land Salzburg", "Tal im Land Salzburg"), Link("Tal in Oberösterreich", "Tal in Oberösterreich"), Link("Tal in Bayern", "Tal in Bayern"), Link("!", "Salzach")))
		)
	}

	def wikipediaDisambiguationPagesSet(): Set[String] = {
		Set("Zerfall", "Fisch", "Zwilling (Begriffsklärung)")
	}

	def wikipediaAbstracts(): Map[String, String] = {
		Map(
			"Audi" -> """Die Audi AG (, Eigenschreibweise: AUDI AG) mit Sitz in Ingolstadt in Bayern ist ein deutscher Automobilhersteller, der dem Volkswagen-Konzern angehört. Der Markenname ist ein Wortspiel zur Umgehung der Namensrechte des ehemaligen Kraftfahrzeugherstellers A. Horch & Cie. Motorwagenwerke Zwickau.""",
			"Electronic Arts" -> """Electronic Arts (EA) ist ein börsennotierter, weltweit operierender Hersteller und Publisher von Computer- und Videospielen. Das Unternehmen wurde vor allem für seine Sportspiele (Madden NFL, FIFA) bekannt, publiziert aber auch zahlreiche andere Titel in weiteren Themengebieten. Ab Mitte der 1990er, bis zu der im Jahr 2008 erfolgten Fusion von Vivendi Games und Activision zu Activision Blizzard, war das Unternehmen nach Umsatz Marktführer im Bereich Computerspiele. Bei einem Jahresumsatz von etwa drei Milliarden Dollar hat das Unternehmen 2007 einen Marktanteil von etwa 25 Prozent auf dem nordamerikanischen und europäischen Markt. Die Aktien des Unternehmens sind im Nasdaq Composite und im S&P 500 gelistet."""
		)
	}

	def rawEntryWithAlternativeWhitespace(): WikipediaEntry = {
		WikipediaEntry("Fehler 2. Art", Option("""Der '''Fehler 2.&nbsp;Art''', auch als '''β-Fehler''' (Beta-Fehler)"""))
	}

	def rawEntryWithStandardWhitespace(): WikipediaEntry = {
		WikipediaEntry("Fehler 2. Art", Option("""Der '''Fehler 2. Art''', auch als '''β-Fehler''' (Beta-Fehler)"""))
	}

	def entryWithAlternativeWhitespace(): WikipediaEntry = {
		WikipediaEntry("Fehler 2. Art", Option("Der Fehler 2.\u00a0Art, auch als β-Fehler (Beta-Fehler) oder Falsch-negativ-Entscheidung bezeichnet, ist ein Fachbegriff der Statistik."))
	}

	def entryWithStandardWhitespaces(): WikipediaEntry = {
		WikipediaEntry("Fehler 2. Art", Option("Der Fehler 2. Art, auch als β-Fehler (Beta-Fehler) oder Falsch-negativ-Entscheidung bezeichnet, ist ein Fachbegriff der Statistik."))
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

	def reducedGroupedAliasesSet(): Set[Alias] = {
		Set(
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

	def reducedGroupedPagesSet(): Set[Page] = {
		Set(
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

	def linkAnalysisArticles(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				"Ingolstadt",
				textlinks = List(
					Link("Ingolstadt", "Ingolstadt", Option(55)),
					Link("Bayern", "Bayern", Option(69)),
					Link("Automobilhersteller", "Automobilhersteller", Option(94)),
					Link("Zerfall", "Zerfall (Album)", Option(4711)),
					Link("Zerfall", "Radioaktivität", Option(4711)),
					Link("Radioaktiver Zerfall", "Radioaktivität", Option(4711)))
			),
			ParsedWikipediaEntry(
				"Bayern",
				textlinksreduced = List(
					Link("Bayern", "Bayern", Option(69)),
					Link("Automobilhersteller", "Automobilhersteller", Option(94)),
					Link("Zerfall", "Zerfall (Album)", Option(4711)),
					Link("Zerfall", "Radioaktivität", Option(4711)),
					Link("Radioaktiver Zerfall", "Radioaktivität", Option(4711)))
			),
			ParsedWikipediaEntry("Automobilhersteller"),
			ParsedWikipediaEntry("Zerfall (Album)"),
			ParsedWikipediaEntry("Radioaktivität")
		)
	}

	def parsedArticlesWithoutLinksSet(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Volvo", Option("lorem")),
			ParsedWikipediaEntry("Opel", Option("ipsum")),
			ParsedWikipediaEntry("Deutsche Bahn", Option("hat Verspätung"))
		)
	}

	def parsedWikipediaSetWithoutText(): Set[ParsedWikipediaEntry] = {
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

	def closedParsedWikipediaSetWithReducedLinks(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi",
				Option("dummy text"),
				textlinksreduced = List(
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

	def germanStopwordsSet(): Set[String] = {
		Set("der", "die", "das", "und", "als", "ist", "an", "am", "im", "dem", "des")
	}

	def unstemmedDFSet(): Set[ParsedWikipediaEntry] = {
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

	def stemmedDFSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("audi", 4),
			DocumentFrequency("backfisch", 3)
		)
	}

	def germanDFStopwordsSet(): Set[DocumentFrequency] = {
		Set(
			DocumentFrequency("ist", 3),
			DocumentFrequency("und", 3)
		)
	}

	def unstemmedGermanWordsList(): List[String] = {
		List("Es", "Keine", "Kein", "Keiner", "Ist", "keine", "keiner", "Streitberg", "Braunschweig",
			"Deutschland", "Baum", "brauchen", "suchen", "könnte")
		// problem words: eine -> eine, hatte -> hatt
	}

	def stemmedGermanWordsList(): List[String] = {
		List("es", "kein", "kein", "kein", "ist", "kein", "kein", "streitberg", "braunschweig",
			"deutschla", "baum", "brauch", "such", "konn")
	}

	def parsedEntry(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Schwarzer Humor",
			Option("""Hickelkasten in Barcelona, Spanien: Der Sprung in den „Himmel“ ist in diesem Fall ein Sprung in den Tod. Hier hat sich jemand einen makabren Scherz erlaubt. Als schwarzer Humor wird Humor bezeichnet, der Verbrechen, Krankheit, Tod und ähnliche Themen, für die gewöhnlich eine Abhandlung in ernster Form erwartet wird, in satirischer oder bewusst verharmlosender Weise verwendet. Oft bezieht er sich auf Zeitthemen. Schwarzer Humor bedient sich häufig paradoxer Stilfiguren. Nicht selten löst er Kontroversen aus darüber, ob man sich über die genannten Dinge lustig machen dürfe und wo die Grenzen des guten Geschmacks lägen; besonders ist dies der Fall, wenn religiöse und sexuelle Themen und tragische Ereignisse zum Gegenstand genommen werden. In der darstellenden Kunst nennt man auf schwarzen Humor setzende Werke schwarze Komödien. Der Begriff wurde durch den Surrealisten André Breton erstmals 1940 in seiner Schrift Anthologie de l’humour noir näher umrissen, wird jedoch seit den 1960er Jahren zum Teil deutlich anders verstanden, indem Kennzeichen der Desillusion und des Nihilismus hinzutraten. In dem Vorwort seines Werkes nennt Breton unter anderem Quellen von Freud und Hegel, die seiner Meinung nach in die Begriffsentwicklung eingeflossen sind. Ursprünge des ‚schwarzen Humors‘ sah Breton in seiner Anthologie bei einigen Werken des irischen Satirikers Jonathan Swift wie Directions to Servants, A Modest Proposal, A Meditation on a Broom-Stick und einige seiner Aphorismen. In den öffentlichen Gebrauch kam der Begriff erst in den 1960er Jahren insbesondere im angloamerikanischen Raum (‚black humour‘) durch die Rezeption von Schriftstellern wie Nathanael West, Vladimir Nabokov und Joseph Heller. So gilt Catch-22 (1961) als ein bekanntes Beispiel dieser Stilart, in dem die Absurdität des Militarismus im Zweiten Weltkrieg satirisch überspitzt wurde. Weitere Beispiele sind Kurt Vonnegut, Slaughterhouse Five (1969), Thomas Pynchon, V. (1963) und Gravity’s Rainbow (1973), sowie im Film Stanley Kubrick’s Dr. Strangelove (1964) und im Absurden Theater insbesondere bei Eugène Ionesco zu finden. Der Begriff black comedy (dtsch. „schwarze Komödie“), der in der englischen Sprache schon für einige Stücke Shakespeares angewandt wurde, weist nach dem Lexikon der Filmbegriffe der Christian-Albrechts-Universität zu Kiel als Komödientyp durch „manchmal sarkastischen, absurden und morbiden ‚schwarzen‘ Humor“ aus, der sich sowohl auf „ernste oder tabuisierte Themen wie Krankheit, Behinderung, Tod, Krieg, Verbrechen“ wie auch auf „für sakrosankt gehaltene Dinge“ richten kann und dabei „auch vor politischen Unkorrektheiten, derben Späßen, sexuellen und skatologischen Anzüglichkeiten nicht zurückschreckt.“ Dabei stehe „hinter der Fassade zynischer Grenzüberschreitungen“ häufig ein „aufrichtiges Anliegen, falsche Hierarchien, Konventionen und Verlogenheiten innerhalb einer Gesellschaft mit den Mitteln filmischer Satire zu entlarven.“ Als filmische Beispiele werden angeführt: Robert Altmans M*A*S*H (USA 1970), Mike Nichols’ Catch-22 (USA 1970), nach Joseph Heller) sowie in der Postmoderne Quentin Tarantinos Pulp Fiction (USA 1994) und Lars von Triers Idioterne (Dänemark 1998). Der Essayist François Bondy schrieb 1971 in Die Zeit: „Der schwarze Humor ist nicht zu verwechseln mit dem ‚kranken Humor‘, der aus den Staaten kam, mit seinen ‚sick jokes‘“ und nannte als Beispiel den Witz: „Mama, ich mag meinen kleinen Bruder nicht. – Schweig, iß, was man dir vorsetzt“. Witz und Humor seien jedoch nicht dasselbe und letzteres „eine originale Geschichte in einer besonderen Tonart“. Humor im Sinne von einer – wie der Duden definiert – „vorgetäuschten Heiterkeit mit der jemand einer unangenehmen oder verzweifelten Lage, in der er sich befindet, zu begegnen“ versucht, nennt man auch Galgenhumor."""),
			textlinks = List(
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
				Link("Galgenhumor", "Galgenhumor", Option(3806))),
			textlinksreduced = List(
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
				Link("Galgenhumor", "Galgenhumor", Option(3806)))
		)
	}

	def parsedEntriesWithLessText(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				"Schwarzer Humor",
				Option("""Hickelkasten in Barcelona, Spanien: Der Sprung in den „Himmel“ ist in diesem Fall ein Sprung in den Tod.""")),
			ParsedWikipediaEntry(
				"Schwarzer Humor",
				Option("""Hickelkasten in Barcelona, Spanien: Der Sprung in den „Himmel“ ist in diesem Fall ein Sprung in den Tod."""),
				rawextendedlinks = List(ExtendedLink("Hickelkasten", Map("Hickelkasten" -> 10), Option(0)))),
			ParsedWikipediaEntry(
				"Schwarzer Humor",
				Option("""Hickelkasten in Barcelona, Spanien: Der Sprung in den „Himmel“ ist in diesem Fall ein Sprung in den Tod."""),
				textlinks = List(Link("Hickelkasten", "Hickelkasten", Option(0)))))
	}

	def foundTrieAliases(): List[List[TrieAlias]] = {
		List(
			List(TrieAlias("Hickelkasten", Option(0))),
			Nil,
			Nil)
	}

	def parsedEntryFoundAliases(): Set[String] = {
		Set(
			"Hickelkasten",
			"Humor",
			"satirischer",
			"paradoxer",
			"Stilfiguren",
			"Kontroversen",
			"guten Geschmacks",
			"Surrealisten",
			"André Breton",
			"Desillusion",
			"Nihilismus",
			"Freud",
			"Hegel",
			"Anthologie",
			"Jonathan Swift",
			"Directions to Servants",
			"A Modest Proposal",
			"A Meditation on a Broom-Stick",
			"Aphorismen",
			"Nathanael West",
			"Vladimir Nabokov",
			"Joseph Heller",
			"Catch-22",
			"Kurt Vonnegut",
			"Slaughterhouse Five",
			"Thomas Pynchon",
			"V.",
			"Gravity’s Rainbow",
			"Stanley Kubrick",
			"Dr. Strangelove",
			"Absurden Theater",
			"Eugène Ionesco",
			"Shakespeares",
			"Christian-Albrechts-Universität zu Kiel",
			"Komödientyp",
			"sarkastischen",
			"absurden",
			"morbiden",
			"tabuisierte",
			"sakrosankt",
			"politischen Unkorrektheiten",
			"sexuellen und skatologischen",
			"zynischer",
			"Satire",
			"Robert Altmans",
			"M*A*S*H",
			"Mike Nichols",
			"Catch-22",
			"Joseph Heller",
			"Postmoderne",
			"Quentin Tarantinos",
			"Pulp Fiction",
			"Lars von Triers",
			"Idioterne",
			"François Bondy",
			"Die Zeit",
			"Witz",
			"Duden",
			"Galgenhumor")
	}

	def namespacePagesList(): List[WikipediaEntry] = {
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
			WikipediaEntry("Medium:Testpage")
		)
	}

	def cleanedNamespacePagesList(): List[WikipediaEntry] = {
		List(
			WikipediaEntry("lol"),
			WikipediaEntry("Audi"),
			WikipediaEntry("Electronic Arts"),
			WikipediaEntry("Postbank-Hochhaus (Berlin)"),
			WikipediaEntry("Postbank-Hochhaus Berlin"),
			WikipediaEntry("Abraham Lincoln"),
			WikipediaEntry("Kategorie:Testpage")
		)
	}

	def namespaceLinksList(): List[Link] = {
		List(
			Link("August Horch", "August Horch", Option(0)),
			Link("August Horch", "Benutzer:August Horch", Option(0)),
			Link("August Horch", "Thema:August Horch", Option(0)),
			Link("August Horch", "HD:August Horch", Option(0)),
			Link("August Horch", "Kategorie:August Horch", Option(0))
		)
	}

	def htmlListLinkPage(): String = {
		Source.fromURL(getClass.getResource("/textmining/test_data.html")).getLines().mkString("\n")
	}

	def extractedListLinksList(): List[Link] = {
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
			Link("Die Zwillinge", "Die Zwillinge")
		)
	}

	def cleanedNamespaceLinksList(): List[Link] = {
		List(
			Link("August Horch", "August Horch", Option(0)),
			Link("August Horch", "Kategorie:August Horch", Option(0))
		)
	}

	def categoryLinksList(): List[Link] = {
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
			Link("Zschopauer", "Zschopau", Option(868))
		)
	}

	def cleanedCategoryLinksList(): List[Link] = {
		List(
			Link("Ingolstadt", "Ingolstadt", Option(55)),
			Link("Bayern", "Bayern", Option(69)),
			Link("Volkswagen", "Volkswagen AG", Option(123)),
			Link("A. Horch & Cie. Motorwagenwerke Zwickau", "Horch", Option(255)),
			Link("August Horch", "August Horch", Option(316)),
			Link("Lateinische", "Latein", Option(599)),
			Link("Imperativ", "Imperativ (Modus)", Option(636)),
			Link("Zschopauer", "Zschopau", Option(868))
		)
	}

	def extractedCategoryLinksList(): List[Link] = {
		List(
			Link("Automobilhersteller", "Automobilhersteller"),
			Link("Wortspiel", "Wortspiel"),
			Link("Namensrechte", "Marke (Recht)"),
			Link("Zwickau", "Zwickau")
		)
	}

	def linksWithRedirectsSet(): Set[Link] = {
		Set(Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus Berlin", Option(10)))
	}

	def linksWithResolvedRedirectsSet(): Set[Link] = {
		Set(Link("Postbank Hochhaus in Berlin", "Postbank-Hochhaus (Berlin)", Option(10)))
	}

	def entriesWithBadRedirectsList(): List[WikipediaEntry] = {
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

	def parsedEntriesWithResolvedRedirectsSet(): Set[ParsedWikipediaEntry] = {
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
			ParsedWikipediaEntry("EU", Option("REDIRECT Europäische Union"), textlinks = List(Link("EU", "Europäische Union")), foundaliases = List("REDIRECT"))
		)
	}

	def smallRedirectMap(): Map[String, String] = {
		Map("Postbank-Hochhaus Berlin" -> "Postbank-Hochhaus (Berlin)")
	}

	def redirectMap(): Map[String, String] = {
		Map(
			"Test Redirect Entry 1" -> "test page",
			"Test Redirect Entry 2" -> "test page 2",
			"EU" -> "Europäische Union"
		)
	}

	def transitiveRedirectMap(): Map[String, String] = {
		Map(
			"Site 1" -> "Site 3",
			"Site 3" -> "Site 6",
			"Site 4" -> "Site 5",
			"Site 5" -> "Site 4",
			"Site 7" -> "Site 7",
			"EU" -> "Europäische Union"
		)
	}

	def cleanedRedirectMap(): Map[String, String] = {
		Map(
			"Site 1" -> "Site 6",
			"Site 3" -> "Site 6",
			"EU" -> "Europäische Union"
		)
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
			ParsedWikipediaEntry("Rapunzel Naturkost", listlinks = List(Link("EU", "EU")))
		)
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
			ParsedWikipediaEntry("Rapunzel Naturkost", listlinks = List(Link("EU", "Europäische Union")))
		)
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

	def dataTrie(tokenizer: IngestionTokenizer, file: String = "/textmining/triealiases"): TrieNode = {
		val trie = new TrieNode()
		val aliasList = Source.fromURL(getClass.getResource(file)).getLines()
		for(alias <- aliasList) {
			trie.append(tokenizer.process(alias))
		}
		trie
	}

	def fullTrieStream(inputFile: String = "/textmining/triealiases")(file: String): ByteArrayInputStream = {
		val aliasStream = Source.fromURL(getClass.getResource(inputFile))
		val trieStream = new ByteArrayOutputStream(1024 * 10)
		LocalTrieBuilder.serializeTrie(aliasStream, trieStream)
		new ByteArrayInputStream(trieStream.toByteArray)
	}

	def docfreqStream(file: String)(file2: String): InputStream = {
		new FileInputStream(new File(getClass.getResource(s"/textmining/$file").toURI))
	}

	def uncleanedFoundAliases(): List[String] = {
		List("Alias 1", "", "Alias 2", "Alias 1", "Alias 3")
	}

	def cleanedFoundAliases(): List[String] = {
		List("Alias 1", "Alias 2", "Alias 1", "Alias 3")
	}

	def binaryTrieStream(): ByteArrayInputStream = {
		val aliasStream = TestData.aliasFileStream()
		val trieStream = new ByteArrayOutputStream(1024)
		LocalTrieBuilder.serializeTrie(aliasStream, trieStream)
		new ByteArrayInputStream(trieStream.toByteArray)
	}

	def linkExtenderPagesSet(): Set[Page] = {
		Set(
			Page("Audi", Map("Audi AG" -> 10, "Audi" -> 10), Map("Audi AG" -> 10, "Audi" -> 10)),
			Page("Bayern", Map("Bayern" -> 10), Map("Bayern" -> 10)),
			Page("VW", Map("Volkswagen AG" -> 10, "VW" -> 10), Map("Volkswagen AG" -> 10, "VW" -> 10)),
			Page("Zerfall (Album)", Map("Zerfall" -> 10), Map("Zerfall" -> 10))
		)
	}

	def linkExtenderPagesMap(): Map[String, Map[String, Int]] = {
		Map(
			"Audi" -> Map("Audi AG" -> 10, "Audi" -> 10),
			"Bayern" -> Map("Bayern" -> 10),
			"VW" -> Map("Volkswagen AG" -> 10, "VW" -> 10, "1337VW" -> 10),
			"Zerfall (Album)" -> Map("Zerfall" -> 10)
		)
	}

	def linkExtenderParsedEntry(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Audi",
			Option("Audi ist Audi AG. VW ist Volkswagen AG"),
			textlinksreduced = List(Link("VW", "VW", Option(18)))
		)
	}

	def linkExtenderFoundPages(): Map[String, Map[String, Int]] = {
		Map(
			"Audi" -> Map("Audi AG" -> 10, "Audi" -> 10),
			"VW" -> Map("Volkswagen AG" -> 10, "VW" -> 10, "1337VW" -> 10)
		)
	}

	def linkExtenderFoundAliases(): Map[String, Map[String, Int]] = {
		Map(
			"Audi" -> Map("Audi" -> 10),
			"Audi AG" -> Map("Audi" -> 10),
			"VW" -> Map("VW" -> 10),
			"Volkswagen AG" -> Map("VW" -> 10),
			"1337VW" -> Map("VW" -> 10)
		)
	}

	def linkExtenderExtendedParsedEntry(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Audi",
				Option("Audi ist Audi AG. VW ist Volkswagen AG"),
				textlinksreduced = List(Link("VW", "VW", Option(18), Map())),
				rawextendedlinks = List(
					ExtendedLink("Audi", Map("Audi" -> 10), Option(0)),
					ExtendedLink("Audi AG", Map("Audi" -> 10), Option(9)),
					ExtendedLink("VW", Map("VW" -> 10), Option(18)),
					ExtendedLink("Volkswagen AG", Map("VW" -> 10), Option(25))
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
			Option("Das heutige Postbank-Hochhaus (früher: Postscheckamt Berlin West (Bln W), seit 1985: Postgiroamt Berlin) ist ein Hochhaus der Postbank am Halleschen Ufer 40–60 und der Großbeerenstraße 2 im Berliner Ortsteil Kreuzberg. Das Postscheckamt von Berlin war ab 1909 in einem Neubau in der Dorotheenstraße 29 (heute: 84), der einen Teil der ehemaligen Markthalle IV integrierte, untergebracht und war bis zum Ende des Zweiten Weltkriegs für den Bereich der Städte Berlin, Frankfurt (Oder), Potsdam, Magdeburg und Stettin zuständig. Aufgrund der Deutschen Teilung wurde das Postscheckamt in der Dorotheenstraße nur noch von der Deutschen Post der DDR genutzt. Für den Westteil von Berlin gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das Postscheckamt West eröffnet. 2014 kaufte die CG-Gruppe das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden. Gottfried Gruner Nach den Plänen des Oberpostdirektors Prosper Lemoine wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23 Geschosse und gehört mit einer Höhe von 89 Metern bis heute zu den höchsten Gebäuden in Berlin. Das Hochhaus besitzt eine Aluminium-Glas-Fassade und wurde im sogenannten „Internationalen Stil“ errichtet. Die Gestaltung des Gebäudes orientiert sich an Mies van der Rohes Seagram Building in New York. Zu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der Große Brunnen von Gottfried Gruner. Er besteht aus 18 Säulen aus Bronze und wurde 1972 in Betrieb genommen. Im Postbank-Hochhaus befinden sich mehrere UKW-Sender, die von Media Broadcast betrieben werden. Die Deutsche Funkturm (DFMG), eine Tochtergesellschaft der Deutschen Telekom AG, stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u. a. folgende Hörfunkprogramme auf Ultrakurzwelle ausgestrahlt: Was ist das für eins Masashi \"Jumbo\" Ozaki?"),
			textlinksreduced = List(
				Link("Hochhaus", "Hochhaus", Option(113)),
				Link("Postbank", "Postbank", Option(126)),
				Link("Berliner", "Berlin", Option(190)),
				Link("Kreuzberg", "Berlin-Kreuzberg", Option(208)),
				Link("Frankfurt (Oder)", "Frankfurt (Oder)", Option(465)),
				Link("Potsdam", "Potsdam", Option(483)),
				Link("Magdeburg", "Magdeburg", Option(492)),
				Link("Stettin", "Stettin", Option(506)),
				Link("Deutschen Post der DDR", "Deutsche Post (DDR)", Option(620)),
				Link("New York.", "New York City", Option(1472)),
				Link("Bronze", "Bronze", Option(1800)),
				Link("Media Broadcast", "Media Broadcast", Option(1906)),
				Link("Deutsche Funkturm", "Deutsche Funkturm", Option(1944)),
				Link("Deutschen Telekom AG", "Deutsche Telekom", Option(1999)),
				Link("Masashi \"Jumbo\" Ozaki", "Masashi \"Jumbo\" Ozaki", Option(2117))
			)
		)
	}

	def sentenceList(): List[Sentence] = {
		List(
			Sentence(
				"Postbank-Hochhaus (Berlin)",
				0,
				"Das heutige Postbank-Hochhaus (früher: Postscheckamt Berlin West (Bln W), seit 1985: Postgiroamt Berlin) ist ein Hochhaus der Postbank am Halleschen Ufer 40–60 und der Großbeerenstraße 2 im Berliner Ortsteil Kreuzberg.",
				List(
					EntityLink("Hochhaus", "Hochhaus", Option(113)),
					EntityLink("Postbank", "Postbank", Option(126)),
					EntityLink("Berliner", "Berlin", Option(190)),
					EntityLink("Kreuzberg", "Berlin-Kreuzberg", Option(208))
				),
				List("Das", "heutige", "Postbank-Hochhaus", "früher", "Postscheckamt", "Berlin", "West", "Bln", "W", "seit", "1985", "Postgiroamt", "Berlin", "ist", "ein", "der", "am", "Halleschen", "Ufer", "40–60", "und", "der", "Großbeerenstraße", "2", "im", "Ortsteil")
			),
			Sentence(
				"Postbank-Hochhaus (Berlin)",
				219,
				"Das Postscheckamt von Berlin war ab 1909 in einem Neubau in der Dorotheenstraße 29 (heute: 84), der einen Teil der ehemaligen Markthalle IV integrierte, untergebracht und war bis zum Ende des Zweiten Weltkriegs für den Bereich der Städte Berlin, Frankfurt (Oder), Potsdam, Magdeburg und Stettin zuständig.",
				List(
					EntityLink("Frankfurt (Oder)", "Frankfurt (Oder)", Option(246)),
					EntityLink("Potsdam", "Potsdam", Option(264)),
					EntityLink("Magdeburg", "Magdeburg", Option(273)),
					EntityLink("Stettin", "Stettin", Option(287))
				),
				List("Das", "Postscheckamt", "von", "Berlin", "war", "ab", "1909", "in", "einem", "Neubau", "in", "der", "Dorotheenstraße", "29", "heute", "84", "der", "einen", "Teil", "der", "ehemaligen", "Markthalle", "IV", "integrierte", "untergebracht", "und", "war", "bis", "zum", "Ende", "des", "Zweiten", "Weltkriegs", "für", "den", "Bereich", "der", "Städte", "Berlin", "und", "zuständig")
			),
			Sentence(
				"Postbank-Hochhaus (Berlin)",
				1940,
				"Die Deutsche Funkturm (DFMG), eine Tochtergesellschaft der Deutschen Telekom AG, stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit.",
				List(
					EntityLink("Deutsche Funkturm", "Deutsche Funkturm", Option(4)),
					EntityLink("Deutschen Telekom AG", "Deutsche Telekom", Option(59))
				),
				List("Die", "DFMG", "eine", "Tochtergesellschaft", "der", "stellt", "dafür", "Standorte", "wie", "das", "Berliner", "Postbank-Hochhaus", "bereit")
			)
		)
	}

	def alternativeSentenceList(): List[Sentence] = {
		List(
			Sentence("Audi", 0, "Audi ist Audi AG.", List(EntityLink("Audi", "Audi", Some(0)), EntityLink("Audi AG", "Audi", Some(9))), List("ist")),
			Sentence("Audi", 18, "VW ist Volkswagen AG", List(EntityLink("VW", "VW", Some(0)), EntityLink("Volkswagen AG", "VW", Some(7))), List("ist")),
			Sentence("Audi", 40, "Audi ist Deutschland.", List(EntityLink("Audi", "Audi", Some(0)), EntityLink("Deutschland", "Deutschland", Some(9))), List("ist")),
			Sentence("Audi", 60, "VW ist in Wolfsburg", List(EntityLink("VW", "VW", Some(0)), EntityLink("Wolfsburg", "Wolfsburg", Some(10))), List("ist"))
		)
	}
	def alternativeSentenceListFiltered(): List[Sentence] = {
		List(
			Sentence("Audi", 0, "Audi ist Audi AG.", List(EntityLink("Audi", "Audi", Some(0)), EntityLink("Audi AG", "Audi", Some(9))), List("ist")),
			Sentence("Audi", 18, "VW ist Volkswagen AG", List(EntityLink("VW", "VW", Some(0)), EntityLink("Volkswagen AG", "VW", Some(7))), List("ist"))
		)
	}

	def entityLists(): List[List[String]] = {
		List(
			List("Hochhaus", "Postbank", "Berlin", "Berlin-Kreuzberg"),
			List("Frankfurt (Oder)", "Potsdam", "Magdeburg", "Stettin"),
			List("Deutsche Funkturm", "Deutsche Telekom")
		)
	}

	def relationList(): List[Relation] = {
		List(
			Relation("Audi", "country", "Deutschland"),
			Relation("Volkswagen", "city", "Wolfsburg")
		)
	}

	def sentencesWithCooccurrences(): List[Sentence] = {
		List(
			Sentence("Audi", 0, "Audi ist Volkswagen.", List(EntityLink("Audi", "Audi", Some(0)), EntityLink("Volkswagen", "Volkswagen AG", Some(1))), List("ist")),
			Sentence("Audi", 18, "Audi ist Volkswagen AG.", List(EntityLink("Audi", "Audi", Some(0)), EntityLink("Volkswagen AG", "Volkswagen AG", Some(1))), List("ist"))
		)
	}

	def cooccurrences(): List[Cooccurrence] = {
		List(
			Cooccurrence(List("Audi", "Volkswagen AG"), 2)
		)
	}

	def bigLinkExtenderPagesSet(): Set[Page] = {
		Set(
			Page("Audi", Map("Audi AG" -> 100, "Audi" -> 100, "Volkswagen AG" -> 2), Map("Audi AG" -> 100, "Audi" -> 100, "Volkswagen AG" -> 2)),
			Page("VW", Map("Volkswagen AG" -> 10, "VW" -> 10), Map("Volkswagen AG" -> 10, "VW" -> 10)),
			Page("Hochhaus", Map("Hochhaus" -> 10, "Gebäude" -> 10), Map("Hochhaus" -> 10, "Gebäude" -> 10)),
			Page("Postbank", Map("Postbank" -> 10), Map("Postbank" -> 10)),
			Page("Berlin", Map("Berlin" -> 10, "(" -> 10, "Berliner" -> 10), Map("Berlin" -> 10, "(" -> 10, "Berliner" -> 10)),
			Page("Berlin-Kreuzberg", Map("Kreuzberg" -> 10), Map("Kreuzberg" -> 10)),
			Page("Frankfurt (Oder)", Map("Frankfurt (Oder)" -> 10), Map("Frankfurt (Oder)" -> 10)),
			Page("Potsdam", Map("Potsdam" -> 10), Map("Potsdam" -> 10)),
			Page("Magdeburg", Map("Magdeburg" -> 10), Map("Magdeburg" -> 10)),
			Page("Stettin", Map("Stettin" -> 10), Map("Stettin" -> 10)),
			Page("Deutsche Post (DDR)", Map("Deutschen Post der DDR" -> 10), Map("Deutschen Post der DDR" -> 10)),
			Page("New York City", Map("New York." -> 10), Map("New York." -> 10)),
			Page("Bronze", Map("Bronze" -> 10), Map("Bronze" -> 10)),
			Page("Media Broadcast", Map("Media Broadcast" -> 10), Map("Media Broadcast" -> 10)),
			Page("Deutsche Funkturm", Map("Deutsche Funkturm" -> 10, "DFMG" -> 10), Map("Deutsche Funkturm" -> 10, "DFMG" -> 10)),
			Page("Deutsche Telekom", Map("Deutschen Telekom AG" -> 10), Map("Deutschen Telekom AG" -> 10)),
			Page("Masashi \"Jumbo\" Ozaki", Map("Masashi \"Jumbo\" Ozaki" -> 10), Map("Masashi \"Jumbo\" Ozaki" -> 10))
		)
	}

	def bigLinkExtenderExtendedParsedEntry(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry(
				"Postbank-Hochhaus (Berlin)",
				Option("Das heutige Postbank-Hochhaus (früher: Postscheckamt Berlin West (Bln W), seit 1985: Postgiroamt Berlin) ist ein Hochhaus der Postbank am Halleschen Ufer 40–60 und der Großbeerenstraße 2 im Berliner Ortsteil Kreuzberg. Das Postscheckamt von Berlin war ab 1909 in einem Neubau in der Dorotheenstraße 29 (heute: 84), der einen Teil der ehemaligen Markthalle IV integrierte, untergebracht und war bis zum Ende des Zweiten Weltkriegs für den Bereich der Städte Berlin, Frankfurt (Oder), Potsdam, Magdeburg und Stettin zuständig. Aufgrund der Deutschen Teilung wurde das Postscheckamt in der Dorotheenstraße nur noch von der Deutschen Post der DDR genutzt. Für den Westteil von Berlin gab es damit zunächst kein eigenes Postscheckamt und daher wurde dort 1948 das Postscheckamt West eröffnet. 2014 kaufte die CG-Gruppe das Gebäude von der Postbank, die das Gebäude als Mieter bis Mitte 2016 weiternutzen will. Nach dem Auszug der Postbank soll das Hochhaus saniert und zu einem Wohn-und Hotelkomplex umgebaut werden. Gottfried Gruner Nach den Plänen des Oberpostdirektors Prosper Lemoine wurde das Gebäude des damaligen Postscheckamtes Berlin West von 1965 bis 1971 errichtet. Es hat 23 Geschosse und gehört mit einer Höhe von 89 Metern bis heute zu den höchsten Gebäuden in Berlin. Das Hochhaus besitzt eine Aluminium-Glas-Fassade und wurde im sogenannten „Internationalen Stil“ errichtet. Die Gestaltung des Gebäudes orientiert sich an Mies van der Rohes Seagram Building in New York. Zu dem Gebäude gehören zwei Anbauten. In dem zweigeschossigen Flachbau waren ein Rechenzentrum und die Schalterhalle untergebracht. In dem sechsgeschossiges Gebäude waren ein Heizwerk und eine Werkstatt untergebracht. Vor dem Hochhaus befindet sich der Große Brunnen von Gottfried Gruner. Er besteht aus 18 Säulen aus Bronze und wurde 1972 in Betrieb genommen. Im Postbank-Hochhaus befinden sich mehrere UKW-Sender, die von Media Broadcast betrieben werden. Die Deutsche Funkturm (DFMG), eine Tochtergesellschaft der Deutschen Telekom AG, stellt dafür Standorte wie das Berliner Postbank-Hochhaus bereit. Über die Antennenträger auf dem Dach werden u. a. folgende Hörfunkprogramme auf Ultrakurzwelle ausgestrahlt:"),
				textlinksreduced = List(
					Link("Hochhaus", "Hochhaus", Option(113)),
					Link("Postbank", "Postbank", Option(126)),
					Link("Berliner", "Berlin", Option(190)),
					Link("Kreuzberg", "Berlin-Kreuzberg", Option(208)),
					Link("Frankfurt (Oder)", "Frankfurt (Oder)", Option(465)),
					Link("Potsdam", "Potsdam", Option(483)),
					Link("Magdeburg", "Magdeburg", Option(492)),
					Link("Stettin", "Stettin", Option(506)),
					Link("Deutschen Post der DDR", "Deutsche Post (DDR)", Option(620)),
					Link("New York.", "New York City", Option(1472)),
					Link("Bronze", "Bronze", Option(1800)),
					Link("Media Broadcast", "Media Broadcast", Option(1906)),
					Link("Deutsche Funkturm", "Deutsche Funkturm", Option(1944)),
					Link("Deutschen Telekom AG", "Deutsche Telekom", Option(1999)),
					Link("Masashi \"Jumbo\" Ozaki", "Masashi \"Jumbo\" Ozaki", Option(2217))
				),
				rawextendedlinks = List(
					ExtendedLink("Berlin", Map("Berlin" -> 10), Option(53)),
					ExtendedLink("Berlin", Map("Berlin" -> 10), Option(97)),
					ExtendedLink("Hochhaus", Map("Hochhaus" -> 10), Option(113)),
					ExtendedLink("Postbank", Map("Postbank" -> 10), Option(126)),
					ExtendedLink("Berliner", Map("Berlin" -> 10), Option(190)),
					ExtendedLink("Kreuzberg", Map("Berlin-Kreuzberg" -> 10), Option(208)),
					ExtendedLink("Berlin", Map("Berlin" -> 10), Option(241)),
					ExtendedLink("Berlin", Map("Berlin" -> 10), Option(457)),
					ExtendedLink("Frankfurt (Oder)", Map("Frankfurt (Oder)" -> 10), Option(465)),
					ExtendedLink("Potsdam", Map("Potsdam" -> 10), Option(483)),
					ExtendedLink("Magdeburg", Map("Magdeburg" -> 10), Option(492)),
					ExtendedLink("Stettin", Map("Stettin" -> 10), Option(506)),
					ExtendedLink("Deutschen Post der DDR", Map("Deutsche Post (DDR)" -> 10), Option(620)),
					ExtendedLink("Berlin", Map("Berlin" -> 10), Option(673)),
					ExtendedLink("Gebäude", Map("Hochhaus" -> 10), Option(818)),
					ExtendedLink("Postbank", Map("Postbank" -> 10), Option(834)),
					ExtendedLink("Gebäude", Map("Hochhaus" -> 10), Option(852)),
					ExtendedLink("Postbank", Map("Postbank" -> 10), Option(925)),
					ExtendedLink("Hochhaus", Map("Hochhaus" -> 10), Option(943)),
					ExtendedLink("Gebäude", Map("Hochhaus" -> 10), Option(1093)),
					ExtendedLink("Berlin", Map("Berlin" -> 10), Option(1131)),
					ExtendedLink("Berlin", Map("Berlin" -> 10), Option(1270)),
					ExtendedLink("Hochhaus", Map("Hochhaus" -> 10), Option(1282)),
					ExtendedLink("New York.", Map("New York City" -> 10), Option(1472)),
					ExtendedLink("Gebäude", Map("Hochhaus" -> 10), Option(1489)),
					ExtendedLink("Gebäude", Map("Hochhaus" -> 10), Option(1639)),
					ExtendedLink("Hochhaus", Map("Hochhaus" -> 10), Option(1708)),
					ExtendedLink("Bronze", Map("Bronze" -> 10), Option(1800)),
					ExtendedLink("Media Broadcast", Map("Media Broadcast" -> 10), Option(1906)),
					ExtendedLink("Deutsche Funkturm", Map("Deutsche Funkturm" -> 10), Option(1944)),
					ExtendedLink("DFMG", Map("Deutsche Funkturm" -> 10), Option(1963)),
					ExtendedLink("Deutschen Telekom AG", Map("Deutsche Telekom" -> 10), Option(1999)),
					ExtendedLink("Berliner", Map("Berlin" -> 10), Option(2052)),
					ExtendedLink("Masashi \"Jumbo\" Ozaki", Map("Masashi \"Jumbo\" Ozaki" -> 10), Option(2217))
				)
			)
		)
	}

	def textLinkHtml(): List[Element] = {
		val elements = Source.fromURL(getClass.getResource("/textmining/textlinks.html"))
			.getLines()
			.toList
			.map(_.replaceAll("\\\\n", "\n"))
			.map(Jsoup.parse)
			.map(_.body)
		elements(3).childNodes.head.asInstanceOf[TextNode].text("\n")
		elements
	}

	def redirectHTML(): List[(String, String, String)] = {
		Source.fromURL(getClass.getResource("/textmining/textlinks.html"))
			.getLines()
			.toList
			.map(_.replaceAll("\\\\n", "\n"))
			.map(("title", _, ""))
	}

	def extractedRedirects(): List[List[Link]] = {
		List(
			List(Link("title", "Zwillinge")),
			List(Link("title", "Zwillinge")),
			Nil,
			Nil,
			List(Link("title", "Zwillinge")))
	}

	def textLinkTuples(): List[(String, List[Link])] = {
		List(
			("Wort Zwillinge", List(Link("Zwillinge", "Zwillinge", Option(5)))),
			("Wort 2", List(Link("Zwillinge", "Zwillinge", Option(7)))),
			("Wort 3", Nil),
			("", Nil),
			("Zwillinge", List(Link("Zwillinge", "Zwillinge", Option(0)))))
	}

	def wikidataEntities(): List[WikidataEntity] = {
		List(
			WikidataEntity("Q1", instancetype = Option("comp"), wikiname = Option("Page 1")),
			WikidataEntity("Q2", instancetype = Option("comp"), wikiname = Option("Page 2")),
			WikidataEntity("Q3", instancetype = Option("comp"), wikiname = Option("Page 3")),
			WikidataEntity("Q4", instancetype = Option("comp")),
			WikidataEntity("Q5", wikiname = Option("Page 5")),
			WikidataEntity("Q6"))
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
				textlinks = List(Link("P1Alias1", "Page 1"), Link("P4Alias3", "Page 4")),
				templatelinks = List(Link("P2Alias1", "Page 2")),
				categorylinks = List(Link("P3Alias1", "Page 3")),
				listlinks = List(Link("P4Alias1", "Page 4")),
				textlinksreduced = List(Link("P1Alias1", "Page 1")),
				templatelinksreduced = List(Link("P2Alias1", "Page 2")),
				categorylinksreduced = List(Link("P3Alias1", "Page 3"))),
			ParsedWikipediaEntry("Title 2",
				textlinks = List(Link("P5Alias1", "Page 5")),
				categorylinks = List(Link("P6Alias1", "Page 6"))),
			ParsedWikipediaEntry(
				"Title 3",
				listlinks = List(Link("P1Alias2", "Page 1")),
				disambiguationlinks = List(Link("P2Alias2", "Page 2")),
				listlinksreduced = List(Link("P1Alias2", "Page 1")),
				disambiguationlinksreduced = List(Link("P2Alias2", "Page 2")))
		)
	}

	def classifierFeatureEntries(offset: Int = 0): List[FeatureEntry] = {
		List(
			FeatureEntry("Audi1", offset, "alias1", "page1", 0.01, MultiFeature(0.01), MultiFeature(0.1), false),
			FeatureEntry("Audi2", offset + 1, "alias2", "page2", 0.3, MultiFeature(0.7), MultiFeature(0.9), true),
			FeatureEntry("Audi3", offset + 2, "alias3", "page3", 0.1, MultiFeature(0.2), MultiFeature(0.3), false),
			FeatureEntry("Audi4", offset + 3, "alias4", "page4", 0.2, MultiFeature(0.1), MultiFeature(0.2), false),
			FeatureEntry("Audi5", offset + 4, "alias5", "page5", 0.05, MultiFeature(0.6), MultiFeature(0.7), true),
			FeatureEntry("Audi6", offset + 5, "alias6", "page6", 0.03, MultiFeature(0.1), MultiFeature(0.3), false),
			FeatureEntry("Audi7", offset + 6, "alias7", "page7", 0.2, MultiFeature(0.7), MultiFeature(0.6), true)
		)
	}

	def classifierFeatureEntriesWithGrouping(offset: Int = 0): List[FeatureEntry] = {
		List(
			FeatureEntry(s"Audi$offset", offset, s"alias$offset", "page1", 0.01, MultiFeature(0.01), MultiFeature(0.1), false),
			FeatureEntry(s"Audi$offset", offset, s"alias$offset", "page2", 0.3, MultiFeature(0.7), MultiFeature(0.9), true),
			FeatureEntry(s"Audi$offset", offset, s"alias$offset", "page3", 0.1, MultiFeature(0.2), MultiFeature(0.3), false),
			FeatureEntry(s"Audi$offset", offset, s"alias$offset", "page4", 0.2, MultiFeature(0.1), MultiFeature(0.2), false),
			FeatureEntry(s"Audi$offset", offset, s"alias$offset", "page5", 0.05, MultiFeature(0.6), MultiFeature(0.7), true),
			FeatureEntry(s"Audi$offset", offset, s"alias$offset", "page6", 0.03, MultiFeature(0.1), MultiFeature(0.3), false),
			FeatureEntry(s"Audi$offset", offset, s"alias$offset", "page7", 0.2, MultiFeature(0.7), MultiFeature(0.6), true)
		)
	}

	def extendedClassifierFeatureEntries(n: Int = 5): List[FeatureEntry] = {
		(0 until n).flatMap(classifierFeatureEntries).toList
	}

	def extendedClassifierFeatureEntriesWithGrouping(n: Int = 5): List[FeatureEntry] = {
		(0 until n).flatMap(classifierFeatureEntriesWithGrouping).toList
	}

	def labeledPredictions(): List[(Double, Double)] = {
		List(
			(1.0, 1.0),
			(1.0, 1.0),
			(1.0, 1.0),
			(1.0, 1.0),
			(1.0, 1.0),
			(0.0, 1.0),
			(1.0, 0.0),
			(1.0, 0.0),
			(0.0, 0.0),
			(0.0, 0.0))
	}

	def badLabeledPredictions(): List[(Double, Double)] = {
		List(
			(0.0, 0.0),
			(0.0, 0.0),
			(0.0, 0.0)
		)
	}

	def predictionStatistics(): PrecisionRecallDataTuple = {
		PrecisionRecallDataTuple(1.0, 5.0 / 7.0, 5.0 / 6.0, 25.0 / 34.0)
	}

	def statisticTuples(): List[PrecisionRecallDataTuple] = {
		List(
			PrecisionRecallDataTuple(1.0, 0.4, 1.0, 1.0),
			PrecisionRecallDataTuple(1.0, 0.6, 0.0, 1.0),
			PrecisionRecallDataTuple(1.0, 0.6, 0.0, 1.0),
			PrecisionRecallDataTuple(1.0, 0.4, 0.0, 0.0)
		)
	}

	def averagedStatisticTuples(): PrecisionRecallDataTuple = {
		PrecisionRecallDataTuple(1.0, 0.5, 0.25, 0.75)
	}

	def simMeasureStats(): SimilarityMeasureStats = {
		SimilarityMeasureStats(null, List(PrecisionRecallDataTuple(1.0, 1.0, 1.0, 1.0)))
	}

	def crossValidationMethod(
		data: RDD[FeatureEntry],
		numFolds: Int = 3,
		classifier: RandomForestClassifier,
		session: SparkSession
	): (SimilarityMeasureStats, Option[PipelineModel]) = {
		(simMeasureStats(), None)
	}

	def formattedPredictionStatistics(): String = {
		s"precision\t1.0\t${5.0 / 7.0}" + "\n" +
			"precision\t0.0\t0.6" + "\n" +
			s"recall\t1.0\t${5.0 / 6.0}" + "\n" +
			"recall\t0.0\t1.0" + "\n" +
			s"fscore with beta 0.5\t1.0\t${25.0 / 34.0}" + "\n" +
			"fscore with beta 0.5\t0.0\t0.6521739130434783"
	}

	def exportDBpediaRelations(): List[Relation] = {
		List(
			Relation("e 1", "r 1", "e 2"),
			Relation("e 1", "r 2", "e 2"),
			Relation("d 1", "r 1", "d 2")
		)
	}

	def exportCooccurrences(): List[Cooccurrence] = {
		List(
			Cooccurrence(List("e 1", "e\" 2", "e 3"), 5),
			Cooccurrence(List("e 1", "d 2"), 2),
			Cooccurrence(List("e 1", "e 3"), 2))
	}

	def nodes(): Set[String] = {
		Set(
			"\"e 1\",\"e 1\",Entity",
			"\"e 2\",\"e 2\",Entity",
			"\"e\\\" 2\",\"e\\\" 2\",Entity",
			"\"e 3\",\"e 3\",Entity",
			"\"d 2\",\"d 2\",Entity",
			"\"d 1\",\"d 1\",Entity"
		)
	}

	def edges(): Set[String] = {
		Set(
			"\"e 1\",5,\"e\\\" 2\",CO_OCCURRENCE",
			"\"e 1\",7,\"e 3\",CO_OCCURRENCE",
			"\"e\\\" 2\",5,\"e 3\",CO_OCCURRENCE",
			"\"e 1\",2,\"d 2\",CO_OCCURRENCE",
			"\"e 1\",r 1,\"e 2\",DBPEDIA",
			"\"e 1\",r 2,\"e 2\",DBPEDIA",
			"\"d 1\",r 1,\"d 2\",DBPEDIA"
		)
	}

	def aliasCounterArticles(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				"Article 1",
				textlinks = List(
					Link("Alias 1", "Page 1", Option(0)),
					Link("Alias 1", "Page 1", Option(1)),
					Link("Alias 2", "Page 2", Option(2))
				),
				foundaliases = List("Alias 1", "Alias 1", "Alias 2", "Alias 3", "Alias 1")
			),
			ParsedWikipediaEntry(
				"Article 2",
				textlinks = List(
					Link("Alias 3", "Page 3", Option(0)),
					Link("Alias 4", "Page 4", Option(1)),
					Link("Alias 5", "Page 5", Option(2))
				),
				foundaliases = List("Alias 1", "Alias 1", "Alias 2", "Alias 3", "Alias 1", "Alias 3", "Alias 4", "Alias 5")
			),
			ParsedWikipediaEntry(
				"Article 3",
				textlinks = List(
					Link("Alias 1", "Page 1", Option(0))
				),
				foundaliases = List("Alias 1", "Alias 1", "Alias 2", "Alias 2")
			)
		)
	}

	def multipleAliasCounts(): Set[(String, Option[Int], Option[Int])] = {
		Set(
			("Alias 1", Option(3), Option(8)),
			("Alias 2", Option(1), Option(4)),
			("Alias 3", Option(1), Option(3)),
			("Alias 4", Option(1), Option(1)),
			("Alias 5", Option(1), Option(1))
		)
	}

	def linkContextExtractionData(): Set[ParsedWikipediaEntry] = {
		Set(
			ParsedWikipediaEntry("Audi Test mit Link", Option("Hier ist Audi verlinkt."),
				textlinks = List(
					Link("Audi", "Audi", Option(9)),
					Link("VW", "VW", Option(1))),
				linkswithcontext = List(
					Link("Audi", "Audi", Option(9), Map("verlink" -> 1)),
					Link("VW", "VW", Option(1)))
			),
			ParsedWikipediaEntry("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt."),
				textlinks = List(),
				linkswithcontext = List()
			),
			ParsedWikipediaEntry("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
				textlinks = List(
					Link("Brachttal", "Brachttal", Option(55)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66)),
					Link("Hessen", "Hessen", Option(87)),
					Link("1377", "1377", Option(225)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546))
				),
				linkswithcontext = List(
					Link("Brachttal", "Brachttal", Option(55), Map("einwohnerzahl" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "hess" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "stamm" -> 1)),
					Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "hess" -> 1, "gemei" -> 1, "streitberg" -> 1)),
					Link("Hessen", "Hessen", Option(87), Map("einwohnerzahl" -> 1, "brachttal" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "270" -> 1, "kleinst" -> 1, "stamm" -> 1, "gemei" -> 1, "main-kinzig-kreis" -> 1, "streitberg" -> 1, "jahr" -> 1)),
					Link("1377", "1377", Option(225), Map("einwohnerzahl" -> 1, "streidtburgk" -> 1, "nachweislich" -> 1, "erwahnung" -> 1, "ortsteil" -> 1, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "stridberg" -> 1, "kleinst" -> 1, "stamm" -> 1, "tauch" -> 1, "1500" -> 1, "namensvaria" -> 1, "red" -> 1)),
					Link("Büdinger Wald", "Büdinger Wald", Option(546), Map("waldrech" -> 1, "19" -> 1, "ort" -> 1, "jahrhu" -> 1, "-lrb-" -> 1, "huterech" -> 1, "eingeburg" -> 1, "-" -> 1, "-rrb-" -> 1, "holx" -> 1, "ortsnam" -> 1, "streitberg" -> 1, "mittelal" -> 1)))
			),
			ParsedWikipediaEntry("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
				textlinks = List(
					Link("Audi", "Audi", Option(7)),
					Link("Brachttal", "Brachttal", Option(13)),
					Link("historisches Jahr", "1377", Option(24))
				),
				linkswithcontext = List(
					Link("Audi", "Audi", Option(7), Map("brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
					Link("Brachttal", "Brachttal", Option(13), Map("audi" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)),
					Link("historisches Jahr", "1377", Option(24), Map("audi" -> 1, "brachttal" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)))
			))
	}

	def articlesWithoutTrieAliases(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				title = "1",
				text = Option("Dieser Artikel schreibt über Audi.")
			),
			ParsedWikipediaEntry(
				title = "2",
				text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG.")
			),
			ParsedWikipediaEntry(
				title = "3",
				text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich."),
				textlinks = List(Link("Volkswagen", "Volkswagen", Option(4)))
			),
			ParsedWikipediaEntry(
				title = "4",
				text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen.")
			)
		)
	}

	def articlesWithTrieAliases(): List[ParsedWikipediaEntry] = {
		List(
			ParsedWikipediaEntry(
				title = "1",
				text = Option("Dieser Artikel schreibt über Audi."),
				triealiases = List(TrieAlias("Audi", Option(29))),
				foundaliases = List("Audi", ".")
			),
			ParsedWikipediaEntry(
				title = "2",
				text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG."),
				triealiases = List(
					TrieAlias("Audi AG", Option(4)),
					TrieAlias("Volkswagen AG", Option(43))
				),
				foundaliases = List("Audi", "Audi AG", "AG", "Volkswagen", "Volkswagen AG", "AG", ".")
			),
			ParsedWikipediaEntry(
				title = "3",
				text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich."),
				foundaliases = List("Volkswagen", "."),
				textlinks = List(Link("Volkswagen", "Volkswagen", Option(4)))
			),
			ParsedWikipediaEntry(
				title = "4",
				text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen."),
				triealiases = List(
					TrieAlias("Audi", Option(20)),
					TrieAlias("Audi AG", Option(46)),
					TrieAlias("Volkswagen", Option(75))
				),
				foundaliases = List("Audi", ".", "Audi", "Audi AG", "AG", ".", "Volkswagen", ".")
			)
		)
	}

	def predictionData(): List[(Double, Double, String)] = {
		List(
			(1.0, 0.0, "p"),
			(1.0, 1.0, "p"),
			(0.0, 0.0, "n"),
			(0.0, 1.0, "n")
		)
	}

	def predictionKeys(): List[String] = {
		List(
			"fp",
			"tp",
			"tn",
			"fn"
		)
	}
}

// scalastyle:on method.length
// scalastyle:on line.size.limit
// scalastyle:on number.of.methods
// scalastyle:on file.size.limit
