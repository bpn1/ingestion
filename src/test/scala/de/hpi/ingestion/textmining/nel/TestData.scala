package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.textmining.models.{Link, TrieAlias, TrieAliasArticle}

// scalastyle:off line.size.limit

object TestData {
	def reducedWikipediaArticles(): Set[TrieAliasArticle] = {
		Set(
			TrieAliasArticle("Audi Test mit Link", Option("Hier ist Audi verlinkt.")),
			TrieAliasArticle("Audi Test ohne Link", Option("Hier ist Audi nicht verlinkt.")),
			TrieAliasArticle("Streitberg (Brachttal)", Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald.""")),
			TrieAliasArticle("Testartikel", Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."))
		)
	}

	def articlesWithFoundLinks(): Set[TrieAliasArticle] = {
		Set(
			TrieAliasArticle("Audi Test mit Link",
				Option("Hier ist Audi verlinkt."),
				List(),
				List(
					Link("Audi", "Audi", Option(9), article = Option("Audi Test mit Link")),
					Link("Audi", "BMW", Option(9), article = Option("Audi Test mit Link")),
					Link("Audi", "Ferrari", Option(9), article = Option("Audi Test mit Link"))
				)),
			TrieAliasArticle("Audi Test ohne Link",
				Option("Hier ist Audi nicht verlinkt."),
				List(),
				List()
			),
			TrieAliasArticle("Streitberg (Brachttal)",
				Option("""Streitberg ist einer von sechs Ortsteilen der Gemeinde Brachttal, Main-Kinzig-Kreis in Hessen. Es ist zugleich der kleinste Ortsteil mit einer Einwohnerzahl von ca. 270. Die erste nachweisliche Erwähnung stammt aus dem Jahre 1377. Im Jahre 1500 ist von Stridberg die Rede, ein Jahr später taucht die Bezeichnung Streidtburgk auf und weitere Namensvarianten sind Stripurgk (1528) und Steytberg (1554). Danach hat sich der Ortsname Streitberg eingebürgert. Vom Mittelalter bis ins 19. Jahrhundert hatte der Ort Waldrechte (Holz- und Huterechte) im Büdinger Wald."""),
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
			TrieAliasArticle("Testartikel",
				Option("Links: Audi, Brachttal, historisches Jahr.\nKeine Links: Hessen, Main-Kinzig-Kreis, Büdinger Wald, Backfisch und nochmal Hessen."),
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

// scalastyle:on line.size.limit