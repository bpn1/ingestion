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

package de.hpi.ingestion.textmining.models

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

// scalastyle:off line.size.limit

object TestData {
    def protoFeatureEntries(): List[(ProtoFeatureEntry, String, MultiFeature)] = {
        List(
            (ProtoFeatureEntry(Link("a", "b", Option(0), article = Option("Audi")), Map(), 0.1, MultiFeature(0.2)), "b", MultiFeature(0.8)),
            (ProtoFeatureEntry(Link("c", "a", Option(10), article = Option("BMW")), Map(), 0.4, MultiFeature(0.0)), "d", MultiFeature(0.7)),
            (ProtoFeatureEntry(Link("e", "f", Option(20), article = Option("Chevrolet")), Map(), 0.7, MultiFeature(1.0)), "f", MultiFeature(0.1)),
            (ProtoFeatureEntry(Link("g", "a", Option(30), article = Option("Dacia")), Map(), 0.8, MultiFeature(0.2)), "h", MultiFeature(0.3)),
            (ProtoFeatureEntry(Link("i", "a", None, article = Option("Ferrari")), Map(), 0.9, MultiFeature(0.4)), "j", MultiFeature(0.7))
        )
    }

    def featureEntries(): List[FeatureEntry] = {
        List(
            FeatureEntry("Audi", 0, "a", "b", 0.1, MultiFeature(0.2), MultiFeature(0.8), true),
            FeatureEntry("BMW", 10, "c", "d", 0.4, MultiFeature(0.0), MultiFeature(0.7), false),
            FeatureEntry("Chevrolet", 20, "e", "f", 0.7, MultiFeature(1.0), MultiFeature(0.1), true),
            FeatureEntry("Dacia", 30, "g", "h", 0.8, MultiFeature(0.2), MultiFeature(0.3), false),
            FeatureEntry("Ferrari", -1, "i", "j", 0.9, MultiFeature(0.4), MultiFeature(0.7), false)
        )
    }

    def labeledPoints(): List[LabeledPoint] = {
        val default = Double.PositiveInfinity
        List(
            LabeledPoint(1.0, new DenseVector(Array(
                0.1,
                0.2, 1, default, default,
                0.8, 1, default, default))),
            LabeledPoint(0.0, new DenseVector(Array(
                0.4,
                0.0, 1, default, default,
                0.7, 1, default, default))),
            LabeledPoint(1.0, new DenseVector(Array(
                0.7,
                1.0, 1, default, default,
                0.1, 1, default, default))),
            LabeledPoint(0.0, new DenseVector(Array(
                0.8,
                0.2, 1, default, default,
                0.3, 1, default, default))),
            LabeledPoint(0.0, new DenseVector(Array(
                0.9,
                0.4, 1, default, default,
                0.7, 1, default, default)))
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
}

// scalastyle:on line.size.limit
