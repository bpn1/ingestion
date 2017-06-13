package de.hpi.ingestion.textmining

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.scalatest.{FlatSpec, Matchers}

class LocalTrieBuilderTest extends FlatSpec with Matchers {

	"Trie" should "be serialized" in {
		val aliasStream = TestData.aliasFileStream()
		val trieStream = new ByteArrayOutputStream(1024)
		LocalTrieBuilder.serializeTrie(aliasStream, trieStream)
		trieStream.size() should be > 0
	}

	"Serialized trie" should "contain the aliases" in {
		val aliasStream = TestData.aliasFileStream()
		val trieStream = new ByteArrayOutputStream(1024)
		LocalTrieBuilder.serializeTrie(aliasStream, trieStream)
		val trieInputStream = new ByteArrayInputStream(trieStream.toByteArray)
		val trie = AliasTrieSearch.deserializeTrie(trieInputStream)
		val expectedTrie = TestData.deserializedTrie()
		trie shouldEqual expectedTrie
	}
}
