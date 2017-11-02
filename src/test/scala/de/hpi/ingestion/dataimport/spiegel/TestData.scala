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

package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.textmining.models.TrieAliasArticle
import play.api.libs.json.{JsObject, Json}

import scala.io.Source

// scalastyle:off line.size.limit
object TestData {
	def spiegelFile(): List[String] = {
		Source.fromURL(getClass.getResource("/spiegel/spiegel.json"))
			.getLines()
			.toList
	}

	def spiegelJson(): List[JsObject] = {
		spiegelFile()
			.map(Json.parse)
			.map(_.as[JsObject])
	}

	def spiegelPages(): List[String] = {
		List(
			"""<div class="spArticleContent">test tag 1</div> abc""",
			"""<div class="dig-artikel">test tag 2</div> abc""",
			"""<div class="article-section">test tag 3</div> abc""",
			"""test no tag"""
		)
	}

	def pageTexts(): List[String] = {
		List(
			"test tag 1",
			"test tag 2",
			"test tag 3",
			"test no tag"
		)
	}

	def parsedArticles(): List[TrieAliasArticle] = {
		List(
			TrieAliasArticle(
				id = "spiegel id 1",
				title = Option("test title 1"),
				text = Option("test title 1 test body 1")
			),
			TrieAliasArticle(
				id = "spiegel id 2",
				title = Option("test title 2"),
				text = Option("test body 2")
			),
			TrieAliasArticle(
				id = "spiegel id 3",
				title = Option("test title 3"),
				text = Option("test title 3")
			),
			TrieAliasArticle(
				id = "spiegel id 4",
				title = None,
				text = Option("abc")
			),
			TrieAliasArticle(
				id = "spiegel id 5",
				title = Option("test title 5"),
				text = None)
		)
	}

	def sentenceArticles(): List[TrieAliasArticle] = {
		List(
			TrieAliasArticle(id = "1", text = Option("Das könnte dem Stellvertreter Osama bin Ladens das Leben gerettet haben. Statt selbst zu kommen schickte Sawahiri Angaben aus pakistanischen Sicherheitskreisen zufolge mehrere Vertreter. Nun werde geprüft, ob sich Sawahiris Helfer unter den Opfern in den drei zerstörten Häusern befunden haben, hieß es. Bei dem Angriff auf das Dorf waren mindestens 17 Menschen getötet worden. DPA Proteste in Pakistan: \"Tod für Amerika\" Erstmals verteidigte US-Außenministerin Condoleezza Rice den Luftangriff heute indirekt. Sie könne sich zu den \"Details dieses speziellen Umstandes\" nicht äußern, sagte Rice in Monrovia. Das afghanisch-pakistanische Grenzgebiet sei aber \"extrem schwierig\".")),
			TrieAliasArticle(id = "2", text = Option("Comic von Jamiri: Eine Frage der Mathematik - SPIEGEL ONLINE Comic von Jamiri Eine Frage der Mathematik Jedes Gramm zählt auf Reisen. Gewicht kostet Benzin, lautet die alte Regel. Wobei coole Köpfe auch das der Beifahrerin einbeziehen - oder des Beifahrers, denn Fahrerinnen können durchaus im Vorteil sein. Freitag, 17.09.2004 11:49 Uhr Drucken Nutzungsrechte Feedback Klicken Sie auf das Bild, um den Comic zu starten. Jamiri zeichnet exklusiv für SPIEGEL ONLINE. Mehr von Jamiri gibt's bei Carlsen Comics."))
		)
	}

	def splitSentences(): Set[String] = {
		Set(
			"""Das könnte dem Stellvertreter Osama bin Ladens das Leben gerettet haben."""
				+ "\n" + """Statt selbst zu kommen schickte Sawahiri Angaben aus pakistanischen Sicherheitskreisen zufolge mehrere Vertreter."""
				+ "\n" + """Nun werde geprüft, ob sich Sawahiris Helfer unter den Opfern in den drei zerstörten Häusern befunden haben, hieß es."""
				+ "\n" + """Bei dem Angriff auf das Dorf waren mindestens 17 Menschen getötet worden."""
				+ "\n" + """DPA Proteste in Pakistan: "Tod für Amerika" Erstmals verteidigte US-Außenministerin Condoleezza Rice den Luftangriff heute indirekt."""
				+ "\n" + """Sie könne sich zu den "Details dieses speziellen Umstandes" nicht äußern, sagte Rice in Monrovia."""
				+ "\n" + """Das afghanisch-pakistanische Grenzgebiet sei aber "extrem schwierig".""",
			"""Comic von Jamiri: Eine Frage der Mathematik - SPIEGEL ONLINE Comic von Jamiri Eine Frage der Mathematik Jedes Gramm zählt auf Reisen."""
				+ "\n" + """Gewicht kostet Benzin, lautet die alte Regel."""
				+ "\n" + """Wobei coole Köpfe auch das der Beifahrerin einbeziehen - oder des Beifahrers, denn Fahrerinnen können durchaus im Vorteil sein."""
				+ "\n" + """Freitag, 17.09.2004 11:49 Uhr Drucken Nutzungsrechte Feedback Klicken Sie auf das Bild, um den Comic zu starten."""
				+ "\n" + """Jamiri zeichnet exklusiv für SPIEGEL ONLINE."""
				+ "\n" + """Mehr von Jamiri gibt's bei Carlsen Comics."""
		)
	}
}
// scalastyle:on line.size.limit
