import org.scalatest.FlatSpec

class TrieTest extends FlatSpec {

	// from https://github.com/mauricio/scala-sandbox/blob/master/src/test/scala/trie/TrieSpec.scala
	"trie" should "include a word" in {
		val trie = new TrieNode()
		trie.append(List[String]("Maurício"))

		assert(trie.contains(List[String]("Maurício")))
	}

	it should "find by prefix" in {
		val trie = new TrieNode()
		trie.append(List[String]("San", "Francisco"))
		trie.append(List[String]("San", "Diego"))
		trie.append(List[String]("San", "to", "Domingo"))
		trie.append(List[String]("São", "Paulo"))

		val result = trie.findByPrefix(List[String]("San"))

		assert(result.length == 3)
		assert(result.contains(List[String]("San", "Francisco")))
		assert(result.contains(List[String]("San", "Diego")))
		assert(result.contains(List[String]("San", "to", "Domingo")))
	}

	it should "not find by prefix if it does not exist" in {
		val trie = new TrieNode()
		trie.append(List[String]("São", "Paulo"))
		trie.append(List[String]("Rio", "de", "Janeiro"))
		trie.append(List[String]("Philadelphia"))

		assert(trie.findByPrefix(List[String]("bos")).isEmpty)
	}

	it should "say that a word is contained" in {
		val trie = new TrieNode()
		trie.append(List[String]("João", "Pessoa"))

		assert(trie.contains(List[String]("João", "Pessoa")))
	}

	it should "say that a word is not contained" in {
		val trie = new TrieNode()
		trie.append(List[String]("João", "Pessoa"))

		assert(!trie.contains(List[String]("João")))
	}

	it should "provide the path to a word" in {
		val word = List[String]("San", "Diego")

		val trie = new TrieNode()
		trie.append(List[String]("San", "Francisco"))
		trie.append(word)

		val list = trie.pathTo(word).get
		assert(list.length == 3) // includes empty word
		(0 until 2).map { index =>
			assert(list(index).children.get(word(index)).isDefined)
		}
	}

	it should "remove an item" in {
		val trie = new TrieNode()
		trie.append(List[String]("San", "Francisco"))
		trie.append(List[String]("San", "Antonio"))
		assert(trie.remove(List[String]("San", "Francisco")))
		assert(!trie.contains(List[String]("San", "Francisco")))
		assert(trie.contains(List[String]("San", "Antonio")))
	}

	it should "remove a longer item" in {
		val name = List[String]("João", "Pessoa")

		val trie = new TrieNode()
		trie.append(List[String]("João"))
		trie.append(name)

		assert(trie.remove(name))

		assert(!trie.contains(name))
		assert(trie.contains(List[String]("João")))
	}

	it should "remove a smaller item" in {
		val name = List[String]("João")

		val trie = new TrieNode()
		trie.append(List[String]("João", "Pessoa"))
		trie.append(name)

		assert(trie.remove(name))

		assert(!trie.contains(name))
		assert(trie.contains(List[String]("João", "Pessoa")))
	}

	it should "not remove if it does not exist" in {
		val trie = new TrieNode()
		trie.append(List[String]("New", "York"))

		assert(!trie.remove(List[String]("Berlin")))
	}

	it should "not remove if it is not a word node" in {
		val trie = new TrieNode()
		trie.append(List[String]("New", "York"))

		assert(!trie.remove(List[String]("New")))
	}

	it should "find matching words" in {
		val trie = new TrieNode()
		trie.append(List[String]("This", "is"))
		trie.append(List[String]("This", "is", "a", "good", "day"))
		trie.append(List[String]("That", "test", "sucks", "a", "lot"))
		val searchTokens = List[String]("This", "is", "a", "good", "day", "to", "do", "shit")
		val matchingWords = trie.matchTokens(searchTokens)
		assert(matchingWords.length == 2)
		assert(matchingWords.head.length <= matchingWords(1).length)
	}


}
