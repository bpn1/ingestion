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

package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.textmining.ClassifierTraining
import de.hpi.ingestion.textmining.TestData.{extendedClassifierFeatureEntries, rfModel}
import de.hpi.ingestion.textmining.models.{TrieAlias, TrieAliasArticle, _}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession

// scalastyle:off line.size.limit
// scalastyle:off method.length

object TestData {
    def reducedWikipediaArticles(): Set[TrieAliasArticle] = {
        Set(
            TrieAliasArticle(
                id = "Audi Test mit Link",
                title = Option("Audi Test mit Link"),
                text = Option("Hier ist Audi verlinkt.")
            ),
            TrieAliasArticle(
                id = "Audi Test ohne Link",
                title = Option("Audi Test ohne Link"),
                text = Option("Hier ist Audi nicht verlinkt.")
            ),
            TrieAliasArticle(
                id = "Streitberg (Brachttal)",
                title = Option("Streitberg (Brachttal)"),
                text = Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald.""")
            ),
            TrieAliasArticle(
                id = "Testartikel",
                title = Option("Testartikel"),
                text = Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen.")
            )
        )
    }

    def articlesWithFoundLinks(): Set[TrieAliasArticle] = {
        Set(
            TrieAliasArticle(
                id = "Audi Test mit Link",
                title = Option("Audi Test mit Link"),
                text = Option("Hier ist Audi verlinkt."),
                List(),
                List(
                    Link("Audi", "Audi", Option(9), article = Option("Audi Test mit Link")),
                    Link("Audi", "BMW", Option(9), article = Option("Audi Test mit Link")),
                    Link("Audi", "Ferrari", Option(9), article = Option("Audi Test mit Link"))
                )),
            TrieAliasArticle(
                id = "Audi Test ohne Link",
                title = Option("Audi Test ohne Link"),
                text = Option("Hier ist Audi nicht verlinkt."),
                List(),
                List()
            ),
            TrieAliasArticle(
                id = "Streitberg (Brachttal)",
                title = Option("Streitberg (Brachttal)"),
                text = Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
                List(),
                List(
                    Link("Brachttal", "Brachttal", Option(55), article = Option("Streitberg (Brachttal)")),
                    Link("Main-Kinzig-Kreis", "Main-Kinzig-Kreis", Option(66), article = Option("Streitberg (Brachttal)")),
                    Link("Hessen", "Hessen", Option(87), article = Option("Streitberg (Brachttal)")),
                    Link("Hessen", "Hessen (Stamm)", Option(87), article = Option("Streitberg (Brachttal)")),
                    Link("Hessen", "Kanton Hessen", Option(87), article = Option("Streitberg (Brachttal)")),
                    Link("1377", "1377", Option(225), article = Option("Streitberg (Brachttal)")),
                    Link("Büdinger Wald", "Büdinger Wald", Option(546), article = Option("Streitberg (Brachttal)"))
                )),
            TrieAliasArticle(
                id = "Testartikel",
                title = Option("Testartikel"),
                text = Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
                List(),
                List(
                    Link("Audi", "Audi", Option(7), article = Option("Testartikel")),
                    Link("Brachttal", "Brachttal", Option(13), article = Option("Testartikel")),
                    Link("historisches Jahr", "1377", Option(24), article = Option("Testartikel")),
                    Link("historisches Jahr", "1996", Option(24), article = Option("Testartikel")),
                    Link("historisches Jahr", "2017", Option(24), article = Option("Testartikel"))
                ))
        )
    }

    def aliasSearchArticles(): List[TrieAliasArticle] = {
        List(
            TrieAliasArticle(
                id = "1",
                title = Option("1"),
                text = Option("Dieser Artikel schreibt über Audi.")
            ),
            TrieAliasArticle(
                id = "2",
                title = Option("2"),
                text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG.")
            ),
            TrieAliasArticle(
                id = "3",
                title = Option("3"),
                text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich.")
            ),
            TrieAliasArticle(
                id = "4",
                title = Option("4"),
                text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen.")
            ),
            TrieAliasArticle(
                id = "5",
                title = Option("5"),
                text = Option("-buch aktuell -Der Audio Verlag DER SPIEGEL Dein")
            ),
            TrieAliasArticle(
                id = "6",
                title = Option("6"),
                text = None
            )
        )
    }

    def realAliasSearchArticles(): List[TrieAliasArticle] = {
        List(
            TrieAliasArticle(
                id = "Audi Test mit Link",
                title = Option("Audi Test mit Link"),
                text = Option("Hier ist Audi verlinkt.")
            ),
            TrieAliasArticle(
                id = "Audi Test ohne Link",
                title = Option("Audi Test ohne Link"),
                text = Option("Hier ist Audi nicht verlinkt.")
            ),
            TrieAliasArticle(
                id = "Streitberg (Brachttal)",
                title = Option("Streitberg (Brachttal)"),
                text = Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald.""")
            ),
            TrieAliasArticle(
                id = "Testartikel",
                title = Option("Testartikel"),
                text = Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen.")
            )
        )
    }

    def foundTrieAliases(): Set[(String, List[TrieAlias])] = {
        Set(
            ("1", List(
                TrieAlias("Audi", Option(29))
            )),
            ("2", List(
                TrieAlias("Audi AG", Option(4)),
                TrieAlias("Volkswagen AG", Option(43))
            )),
            ("3", List(
                TrieAlias("Volkswagen", Option(4))
            )),
            ("4", List(
                TrieAlias("Audi", Option(20)),
                TrieAlias("Audi AG", Option(46)),
                TrieAlias("Volkswagen", Option(75))
            )),
            ("5", List(
                TrieAlias("Der Audio Verlag", Option(15))
            )),
            ("6", List())
        )
    }

    def realFoundTrieAliases(): Set[(String, List[TrieAlias])] = {
        Set(
            ("Audi Test mit Link", List(
                TrieAlias("Audi", Option(9))
            )),
            ("Audi Test ohne Link", List(
                TrieAlias("Audi", Option(9))
            )),
            ("Streitberg (Brachttal)", List(
                TrieAlias("Brachttal", Option(55)),
                TrieAlias("Main-Kinzig-Kreis", Option(66)),
                TrieAlias("Hessen", Option(87)),
                TrieAlias("1377", Option(225)),
                TrieAlias("Büdinger Wald", Option(546))
            )),
            ("Testartikel", List(
                TrieAlias("Audi", Option(7)),
                TrieAlias("Brachttal", Option(13)),
                TrieAlias("historisches Jahr", Option(24)),
                TrieAlias("Hessen", Option(56)),
                TrieAlias("Main-Kinzig-Kreis", Option(64)),
                TrieAlias("Büdinger Wald", Option(83)),
                TrieAlias("Hessen", Option(120))
            ))
        )
    }

    def foundAliasArticles(): List[TrieAliasArticle] = {
        List(
            TrieAliasArticle(
                id = "1",
                title = Option("1"),
                text = Option("Dieser Artikel schreibt über Audi."),
                triealiases = List(
                    TrieAlias("Audi", Option(29))
                )
            ),
            TrieAliasArticle(
                id = "2",
                title = Option("2"),
                text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG."),
                triealiases = List(
                    TrieAlias("Audi AG", Option(4)),
                    TrieAlias("Volkswagen AG", Option(43))
                )
            ),
            TrieAliasArticle(
                id = "3",
                title = Option("3"),
                text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich."),
                triealiases = List(
                    TrieAlias("Volkswagen", Option(4))
                )
            ),
            TrieAliasArticle(
                id = "4",
                title = Option("4"),
                text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen."),
                triealiases = List(
                    TrieAlias("Audi", Option(20)),
                    TrieAlias("Audi AG", Option(46)),
                    TrieAlias("Volkswagen", Option(75))
                )
            ),
            TrieAliasArticle(
                id = "5",
                title = Option("5"),
                text = Option("-buch aktuell -Der Audio Verlag DER SPIEGEL Dein"),
                triealiases = List(
                    TrieAlias("Der Audio Verlag", Option(15))
                )
            ),
            TrieAliasArticle(
                id = "6",
                title = Option("6"),
                text = None
            )
        )
    }

    def incompleteFoundAliasArticles(): List[TrieAliasArticle] = {
        List(
            TrieAliasArticle(
                id = "Audi Test ohne Link",
                title = Option("Audi Test ohne Link"),
                text = Option("Hier ist Audi nicht verlinkt."),
                List(
                    TrieAlias("Audi", Option(9))
                )
            ),
            TrieAliasArticle(
                id = "Testartikel",
                title = Option("Testartikel"),
                text = Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
                List(
                    TrieAlias("Audi", Option(7)),
                    TrieAlias("Brachttal", Option(13)),
                    TrieAlias("historisches Jahr", Option(24)),
                    TrieAlias("Hessen", Option(56)),
                    TrieAlias("Main-Kinzig-Kreis", Option(64)),
                    TrieAlias("Büdinger Wald", Option(83)),
                    TrieAlias("Hessen", Option(120))
                )
            ),
            TrieAliasArticle(
                id = "Toan Anh",
                title = Option("Toan Anh"),
                text = Option("REDIRECT Van Toan Nguyen"),
                List(
                    TrieAlias("Van", Option(9))
                )
            )
        )
    }

    def contextArticles(): List[ArticleTfIdf] = {
        List(
            ArticleTfIdf(
                "Audi Test mit Link",
                Map("audi" -> 1, "verlink" -> 1)
            ),
            ArticleTfIdf(
                "Audi Test ohne Link",
                Map("audi" -> 1, "verlink" -> 1)
            ),
            ArticleTfIdf(
                "Streitberg (Brachttal)",
                Map("1554" -> 1, "waldrech" -> 1, "einwohnerzahl" -> 1, "streidtburgk" -> 1, "19" -> 1, "brachttal" -> 1, "ort" -> 1, "jahrhu" -> 1, "nachweislich" -> 1, "-lrb-" -> 3, "huterech" -> 1, "eingeburg" -> 1, "steytberg" -> 1, "erwahnung" -> 1, "ortsteil" -> 2, "bezeichnung" -> 1, "jahr" -> 3, "270" -> 1, "-" -> 1, "stridberg" -> 1, "kleinst" -> 1, "-rrb-" -> 3, "stamm" -> 1, "hess" -> 1, "holx" -> 1, "buding" -> 1, "tauch" -> 1, "stripurgk" -> 1, "1500" -> 1, "gemei" -> 1, "1377" -> 1, "wald" -> 1, "main-kinzig-kreis" -> 1, "1528" -> 1, "namensvaria" -> 1, "ortsnam" -> 1, "streitberg" -> 2, "mittelal" -> 1, "red" -> 1, "stamm" -> 1)
            ),
            ArticleTfIdf(
                "Testartikel",
                Map("audi" -> 1, "brachttal" -> 1, "historisch" -> 1, "jahr" -> 1, "hess" -> 2, "main-kinzig-kreis" -> 1, "buding" -> 1, "wald" -> 1, "backfisch" -> 1, "nochmal" -> 1)
            ),
            ArticleTfIdf(
                "Toan Anh",
                Map("nguy" -> 1, "redirec" -> 1, "toa" -> 1, "van" -> 1)
            ),
            ArticleTfIdf("Chevrolet"),
            ArticleTfIdf("Chevrolet Van"),
            ArticleTfIdf("Flughafen Ferit Melen"),
            ArticleTfIdf("Kastenwagen"),
            ArticleTfIdf("Reliant"),
            ArticleTfIdf("Reliant Van"),
            ArticleTfIdf("Toyota"),
            ArticleTfIdf("Tušpa"),
            ArticleTfIdf("Türkisch Van"),
            ArticleTfIdf("Van"),
            ArticleTfIdf("Van (Automobil)"),
            ArticleTfIdf("Van (Provinz)"),
            ArticleTfIdf("Van (Türkei)"),
            ArticleTfIdf("Vansee"),
            ArticleTfIdf("Vilâyet Van")
        )
    }

    def alias(): Alias = {
        Alias(
            "Van",
            Map("Chevrolet" -> 1, "Chevrolet Van" -> 9, "Flughafen Ferit Melen" -> 1, "Kastenwagen" -> 1, "Reliant" -> 7, "Reliant Van" -> 2, "Toyota" -> 1, "Tušpa" -> 2, "Türkisch Van" -> 12, "Van" -> 159, "Van (Automobil)" -> 443, "Van (Provinz)" -> 83, "Van (Türkei)" -> 292, "Vansee" -> 5, "Vilâyet Van" -> 13),
            linkoccurrences = Option(1040),
            totaloccurrences = Option(20508)
        )
    }

    def aliasContexts(): List[(Link, Bag[String, Int])] = {
        List(
            (Link("Audi", null, Option(29), Map(), Option("1")), Bag("artikel" -> 1, "schreib" -> 1, "." -> 1)),
            (Link("Audi AG", null, Option(4), Map(), Option("2")), Bag("tochterunternehm" -> 1, "volkswag" -> 1, "ag" -> 1, "." -> 1)),
            (Link("Volkswagen AG", null, Option(43), Map(), Option("2")), Bag("audi" -> 1, "tochterunternehm" -> 1, "ag" -> 1, "." -> 1)),
            (Link("Volkswagen", null, Option(4), Map(), Option("3")), Bag("aktiengesellschaf" -> 1, "umweltfreundlich" -> 1, "." -> 1)),
            (Link("Audi", null, Option(20), Map(), Option("4")), Bag("audi" -> 1, "ag" -> 1, "satx" -> 3, "enthal" -> 3, "volkswag" -> 1, "." -> 3)),
            (Link("Audi AG", null, Option(46), Map(), Option("4")), Bag("audi" -> 1, "satx" -> 3, "enthal" -> 3, "volkswag" -> 1, "." -> 3)),
            (Link("Volkswagen", null, Option(75), Map(), Option("4")), Bag("audi" -> 2, "ag" -> 1, "satx" -> 3, "enthal" -> 3, "." -> 3)),
            (Link("Der Audio Verlag", null, Option(15), Map(), Option("5")), Bag("der" -> 1, "buch" -> 1, "aktuell" -> 1, "-" -> 2, "spiegel" -> 1))
        )
    }

    def aliasMap(): Map[String, List[(String, Double, Double)]] = {
        Map(
            "Audi" -> List(("Audi", 1.0, 1.0)),
            "Audi AG" -> List(("Audi", 1.0, 1.0)),
            "Volkswagen" -> List(("Volkswagen", 1.0, 1.0)),
            "Volkswagen AG" -> List(("Volkswagen", 1.0, 1.0)),
            "Der Audio Verlag" -> List(("Der Audio Verlag", 1.0, 1.0))
        )
    }

    def rawAliases(): List[Alias] = {
        List(
            Alias("Audi", Map("Audi" -> 1), Map("Audi" -> 1), Option(1), Option(1)),
            Alias("Audi AG", Map("Audi" -> 1), Map("Audi" -> 1), Option(1), Option(1)),
            Alias("Volkswagen", Map("Volkswagen" -> 1), Map("Volkswagen" -> 1), Option(1), Option(1)),
            Alias("Volkswagen AG", Map("Volkswagen" -> 1), Map("Volkswagen" -> 1), Option(1), Option(1)),
            Alias("Der Audio Verlag", Map("Der Audio Verlag" -> 1), Map("Der Audio Verlag" -> 1), Option(1), Option(1))
        )
    }

    def tfidfArticles(): List[ArticleTfIdf] = {
        List(
            ArticleTfIdf("Audi"),
            ArticleTfIdf("Volkswagen"),
            ArticleTfIdf("Der Audio Verlag")
        )
    }

    def featureEntries(): List[FeatureEntry] = {
        List(
            FeatureEntry("1", 29, "Audi", "Audi", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false),
            FeatureEntry("2", 4, "Audi AG", "Audi", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false),
            FeatureEntry("2", 43, "Volkswagen AG", "Volkswagen", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false),
            FeatureEntry("3", 4, "Volkswagen", "Volkswagen", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false),
            FeatureEntry("4", 20, "Audi", "Audi", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false),
            FeatureEntry("4", 46, "Audi AG", "Audi", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false),
            FeatureEntry("4", 75, "Volkswagen", "Volkswagen", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false),
            FeatureEntry("5", 15, "Der Audio Verlag", "Der Audio Verlag", 1.0, MultiFeature(1.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), MultiFeature(0.0, 1, Double.PositiveInfinity, Double.PositiveInfinity), false)
        )
    }

    def linkedEntities(): Set[(String, List[Link])] = {
        Set(
            ("1", List(Link("Audi", "Audi", Option(29)))),
            ("2", List(
                Link("Audi AG", "Audi", Option(4)),
                Link("Volkswagen AG", "Volkswagen", Option(43))
            )),
            ("3", List(Link("Volkswagen", "Volkswagen", Option(4)))),
            ("4", List(
                Link("Audi", "Audi", Option(20)),
                Link("Audi AG", "Audi", Option(46)),
                Link("Volkswagen", "Volkswagen", Option(75))
            )),
            ("5", List(Link("Der Audio Verlag", "Der Audio Verlag", Option(15))))
        )
    }

    def linkedEntitiesForAllArticles(): Set[(String, List[Link])] = {
        Set(
            ("1", List(Link("Audi", "Audi", Option(29)))),
            ("2", List(
                Link("Audi AG", "Audi", Option(4)),
                Link("Volkswagen AG", "Volkswagen", Option(43))
            )),
            ("3", List(Link("Volkswagen", "Volkswagen", Option(4)))),
            ("4", List(
                Link("Audi", "Audi", Option(20)),
                Link("Audi AG", "Audi", Option(46)),
                Link("Volkswagen", "Volkswagen", Option(75))
            )),
            ("5", List(Link("Der Audio Verlag", "Der Audio Verlag", Option(15))))
        )
    }

    def linkedEntitiesForIncompleteArticles(): Set[(String, Set[Link])] = {
        Set(
            ("Audi Test ohne Link", Set(
                Link("Audi", "Audi Test mit Link", Option(9))
            )),
            ("Testartikel", Set(
                Link("Audi", "Audi Test mit Link", Option(7)),
                Link("Brachttal", "Audi Test ohne Link", Option(13)),
                Link("historisches Jahr", "Testartikel", Option(24)),
                Link("Hessen", "Streitberg (Brachttal)", Option(56)),
                Link("Main-Kinzig-Kreis", "Streitberg (Brachttal)", Option(64)),
                Link("Büdinger Wald", "Testartikel", Option(83)),
                Link("Hessen", "Streitberg (Brachttal)", Option(120))
            )),
            ("Toan Anh", Set(
                Link("Van", "Van (Automobil)", Option(9)),
                Link("Van", "Van (Türkei)", Option(9))
            ))
        )
    }

    def randomForestModel(session: SparkSession): () => PipelineModel = {
        if(rfModel.isEmpty) {
            val featureEntries = session.sparkContext.parallelize(extendedClassifierFeatureEntries(10))
            val training = ClassifierTraining.labeledPointDF(featureEntries, session)
            val classifier = ClassifierTraining.randomForestDFModel(1, 2, 1)
                .setFeaturesCol("features")
            val labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(training)
            val labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels)
            val pipeline = new Pipeline().setStages(Array(labelIndexer, classifier, labelConverter))
            rfModel = Option(pipeline.fit(training))
        }
        () => rfModel.get
    }
}

// scalastyle:on method.length
// scalastyle:on line.size.limit
