package de.hpi.ingestion.textmining

import de.hpi.ingestion.textmining.models.{Trie, TrieNode}
import org.scalatest.{FlatSpec, Matchers}

class TrieTest extends FlatSpec with Matchers {

	// from https://github.com/mauricio/scala-sandbox/blob/master/src/test/scala/trie/TrieSpec.scala
	"Trie" should "include a word" in {
		val trie = Trie()
		trie.append(List[String]("Maurício"))

		trie should contain (List[String]("Maurício"))
	}

	it should "find by prefix" in {
		val trie = Trie()
		trie.append(List[String]("San", "Francisco"))
		trie.append(List[String]("San", "Diego"))
		trie.append(List[String]("San", "to", "Domingo"))
		trie.append(List[String]("São", "Paulo"))

		val result = trie.findByPrefix(List[String]("San"))

		result should have length 3
		result should contain (List[String]("San", "Francisco"))
		result should contain (List[String]("San", "Diego"))
		result should contain (List[String]("San", "to", "Domingo"))
	}

	it should "not find by prefix if it does not exist" in {
		val trie = Trie()
		trie.append(List[String]("São", "Paulo"))
		trie.append(List[String]("Rio", "de", "Janeiro"))
		trie.append(List[String]("Philadelphia"))

		trie.findByPrefix(List[String]("bos")) shouldBe empty
	}

	it should "say that a word is contained" in {
		val trie = Trie()
		trie.append(List[String]("João", "Pessoa"))

		trie should contain (List[String]("João", "Pessoa"))
	}

	it should "say that a word is not contained" in {
		val trie = Trie()
		trie.append(List[String]("João", "Pessoa"))

		trie should not contain List[String]("João")
	}

	it should "provide the path to a word" in {
		val word = List[String]("San", "Diego")

		val trie = Trie()
		trie.append(List[String]("San", "Francisco"))
		trie.append(word)

		val list = trie.pathTo(word).get
		list should have length 3 // includes empty word
		(0 until 2).map { index =>
			list(index).children.get(word(index)) shouldBe defined
		}
	}

	it should "remove an item" in {
		val trie = Trie()
		trie.append(List[String]("San", "Francisco"))
		trie.append(List[String]("San", "Antonio"))

		trie.remove(List[String]("San", "Francisco")) shouldBe true
		trie should not contain List[String]("San", "Francisco")
		trie should contain (List[String]("San", "Antonio"))
	}

	it should "remove a longer item" in {
		val name = List[String]("João", "Pessoa")

		val trie = Trie()
		trie.append(List[String]("João"))
		trie.append(name)

		trie.remove(name) shouldBe true
		trie should not contain name
		trie.contains(name) shouldBe false
		trie should contain (List[String]("João"))
	}

	it should "remove a smaller item" in {
		val name = List[String]("João")

		val trie = Trie()
		trie.append(List[String]("João", "Pessoa"))
		trie.append(name)

		trie.remove(name) shouldBe true
		trie should not contain name
		trie.contains(name) shouldBe false
		trie should contain (List[String]("João", "Pessoa"))
		trie.contains(List[String]("João", "Pessoa")) shouldBe true
	}

	it should "not remove if it does not exist" in {
		val trie = Trie()
		trie.append(List[String]("New", "York"))

		trie.remove(List[String]("Berlin")) shouldBe false
	}

	it should "not remove if it is not a word node" in {
		val trie = Trie()
		trie.append(List[String]("New", "York"))

		trie.remove(List[String]("New")) shouldBe false
	}

	it should "find matching words" in {
		val trie = Trie()
		trie.append(List[String]("This", "is"))
		trie.append(List[String]("This", "is", "a", "good", "day"))
		trie.append(List[String]("This", "is", "a", "good", "day", "to", "do", "shit", "haha"))
		trie.append(List[String]("That", "test", "sucks", "a", "lot"))
		val matchingWords = trie.matchTokens("This is a good day to do shit".split(" ").toList)

		matchingWords should have length 2
		matchingWords.head.length should be <= matchingWords(1).length
	}

	"Stringify" should "display token and word" in {
		val trie1 = Trie()
		val stringify1 = trie1.toString()
		val expectedStringify1 = "Trie(token=None,word=None)"
		val trie2 = new TrieNode()
		trie2.append(List[String]("This"))
		val stringify2 = trie2.children("This").toString()
		val expectedStringify2 = "Trie(token=Some(This),word=Some(List(This)))"
		stringify1 shouldEqual expectedStringify1
		stringify2 shouldEqual expectedStringify2
	}

	"Equals and hash code" should "work properly" in {
		val trie1 = Trie()
		trie1.append(List("word", "1"))
		val trie2 = Trie()
		trie2.append(List("word", "2"))
		val trie3 = Trie()
		trie3.append(List("word", "1"))
		val object4 = List("a")

		trie1 shouldEqual trie3
		trie1.hashCode() shouldEqual trie3.hashCode()
		trie1 should not equal trie2
		trie1.equals(trie2) shouldBe false
		trie1.equals(object4) shouldBe false
		trie1.hashCode() should not equal trie2.hashCode()
	}

}
