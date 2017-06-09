package de.hpi.ingestion.textmining.models

import org.scalatest.{FlatSpec, Matchers}

class TrieAliasTest extends FlatSpec with Matchers {

	"Trie Alias" should "be transformed into a link" in {
		val aliasLinks = TestData.trieAliases().map(_.toLink())
		val expectedLinks = TestData.trieAliasLinks()
		aliasLinks shouldEqual expectedLinks
	}
}
