package de.hpi.ingestion.textmining.models

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

object TestData {

	def protoFeatureEntries(): List[(ProtoFeatureEntry, String, MultiFeature)] = {
		val mfLink_09 = MultiFeature(0.9, 1, 0.0, 0.1)
		val mfLink_08 = MultiFeature(0.8, 2, 0.1, 0.1)
		val mfLink_07 = MultiFeature(0.7, 3, 0.2, 0.3)
		val mfLink_04 = MultiFeature(0.4, 4, 0.5, 0.3)
		val mfLink_01 = MultiFeature(0.1, 5, 0.8, 0.0)

		List(
			(ProtoFeatureEntry(Link("a", "b"), Map(), mfLink_01, MultiFeature(0.2)), "b", MultiFeature(0.8)),
			(ProtoFeatureEntry(Link("c", "a"), Map(), mfLink_04, MultiFeature(0.0)), "d", MultiFeature(0.7)),
			(ProtoFeatureEntry(Link("e", "f"), Map(), mfLink_07, MultiFeature(1.0)), "f", MultiFeature(0.1)),
			(ProtoFeatureEntry(Link("g", "a"), Map(), mfLink_08, MultiFeature(0.2)), "h", MultiFeature(0.3)),
			(ProtoFeatureEntry(Link("i", "a"), Map(), mfLink_09, MultiFeature(0.4)), "j", MultiFeature(0.7))
			)
	}

	def featureEntries(): List[FeatureEntry] = {
		val mfLink_09 = MultiFeature(0.9, 1, 0.0, 0.1)
		val mfLink_08 = MultiFeature(0.8, 2, 0.1, 0.1)
		val mfLink_07 = MultiFeature(0.7, 3, 0.2, 0.3)
		val mfLink_04 = MultiFeature(0.4, 4, 0.5, 0.3)
		val mfLink_01 = MultiFeature(0.1, 5, 0.8, 0.0)

		List(
			FeatureEntry("a", "b", mfLink_01, MultiFeature(0.2), MultiFeature(0.8), true, null),
			FeatureEntry("c", "d", mfLink_04, MultiFeature(0.0), MultiFeature(0.7), false, null),
			FeatureEntry("e", "f", mfLink_07, MultiFeature(1.0), MultiFeature(0.1), true, null),
			FeatureEntry("g", "h", mfLink_08, MultiFeature(0.2), MultiFeature(0.3), false, null),
			FeatureEntry("i", "j", mfLink_09, MultiFeature(0.4), MultiFeature(0.7), false, null)
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

	def edgeCaseExtendedLinksToLinks(): List[Link] = {
		List(
			Link("Berlin", "Berlin", Option(0)),
			Link("Berlin", "Berlin", Option(1)),
			Link("Berlin", "Berlin", Option(2))
		)
	}
}
