package de.hpi.ingestion.textmining.models

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

object TestData {
	def protoFeatureEntries(): List[(ProtoFeatureEntry, String, Double)] = {
		List(
			(ProtoFeatureEntry("a", Link("a", "b"), Map(), 0.1, 0.2), "b", 0.8),
			(ProtoFeatureEntry("c", Link("c", "a"), Map(), 0.4, 0.0), "d", 0.7),
			(ProtoFeatureEntry("e", Link("e", "f"), Map(), 0.7, 1.0), "f", 0.1),
			(ProtoFeatureEntry("g", Link("g", "a"), Map(), 0.8, 0.2), "h", 0.3),
			(ProtoFeatureEntry("i", Link("i", "a"), Map(), 0.9, 0.4), "j", 0.7))
	}

	def featureEntries(): List[FeatureEntry] = {
		List(
			FeatureEntry("a", "b", 0.1, 0.2, 0.8, true, null),
			FeatureEntry("c", "d", 0.4, 0.0, 0.7, false, null),
			FeatureEntry("e", "f", 0.7, 1.0, 0.1, true, null),
			FeatureEntry("g", "h", 0.8, 0.2, 0.3, false, null),
			FeatureEntry("i", "j", 0.9, 0.4, 0.7, false, null))
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
			disambiguationlinks = List(Link("Esel", "Esel"))
		)
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
			Link("Esel", "Esel")
		)
	}

	def parsedEntryWithContext(): ParsedWikipediaEntry = {
		ParsedWikipediaEntry(
			"Test Article",
			textlinks = List(
				Link("Hochhaus", "Hochhaus", Option(113)),
				Link("Berliner", "Berlin", Option(190))),
			extendedlinks = List(
				Link("Postbank", "Postbank", Option(126)),
				Link("Kreuzberg", "Berlin-Kreuzberg", Option(208))),
			linkswithcontext = List(
				Link("Hochhaus", "Hochhaus", Option(113), Map("test 1" -> 1)),
				Link("Postbank", "Postbank", Option(126), Map("test 2" -> 2)),
				Link("Berliner", "Berlin", Option(190), Map("test 3" -> 3)),
				Link("Kreuzberg", "Berlin-Kreuzberg", Option(208), Map("test 4" -> 4)))
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
}
