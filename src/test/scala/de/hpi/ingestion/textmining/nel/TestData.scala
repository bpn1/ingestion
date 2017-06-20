package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.textmining.models.{TrieAlias, TrieAliasArticle}

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
			)
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
			)
		)
	}
}
