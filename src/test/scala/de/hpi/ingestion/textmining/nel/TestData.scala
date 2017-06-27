package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.textmining.models._
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node, Predict, RandomForestModel}

// scalastyle:off line.size.limit
object TestData {
	def aliasSearchArticles(): List[TrieAliasArticle] = {
		List(
			TrieAliasArticle(
				article = "1",
				text = Option("Dieser Artikel schreibt über Audi.")
			),
			TrieAliasArticle(
				article = "2",
				text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG.")
			),
			TrieAliasArticle(
				article = "3",
				text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich.")
			),
			TrieAliasArticle(
				article = "4",
				text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen.")
			),
			TrieAliasArticle(
				article = "5",
				text = Option("-buch aktuell -Der Audio Verlag DER SPIEGEL Dein")
			),
			TrieAliasArticle(
				article = "6",
				text = None
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

	def foundAliasArticles(): List[TrieAliasArticle] = {
		List(
			TrieAliasArticle(
				article = "1",
				text = Option("Dieser Artikel schreibt über Audi."),
				triealiases = List(
					TrieAlias("Audi", Option(29))
				)
			),
			TrieAliasArticle(
				article = "2",
				text = Option("Die Audi AG ist ein Tochterunternehmen der Volkswagen AG."),
				triealiases = List(
					TrieAlias("Audi AG", Option(4)),
					TrieAlias("Volkswagen AG", Option(43))
				)
			),
			TrieAliasArticle(
				article = "3",
				text = Option("Die Volkswagen Aktiengesellschaft ist nicht sehr umweltfreundlich."),
				triealiases = List(
					TrieAlias("Volkswagen", Option(4))
				)
			),
			TrieAliasArticle(
				article = "4",
				text = Option("Dieser Satz enthält Audi. Dieser Satz enthält Audi AG. Dieser Satz enthält Volkswagen."),
				triealiases = List(
					TrieAlias("Audi", Option(20)),
					TrieAlias("Audi AG", Option(46)),
					TrieAlias("Volkswagen", Option(75))
				)
			),
			TrieAliasArticle(
				article = "5",
				text = Option("-buch aktuell -Der Audio Verlag DER SPIEGEL Dein"),
				triealiases = List(
					TrieAlias("Der Audio Verlag", Option(15))
				)
			),
			TrieAliasArticle(
				article = "6",
				text = None
			)
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
}

// scalastyle:on line.size.limit
