import org.scalatest.FlatSpec

class TrieTest extends FlatSpec {

	// from https://github.com/mauricio/scala-sandbox/blob/master/src/test/scala/trie/TrieSpec.scala
	"trie" should "include a word" in {
		val trie = new TrieNode()
		trie.append(List[String]("Maurício"))

		assert(trie.contains(List[String]("Maurício")))
	}

	"trie" should "find by prefix" in {
		val trie = new TrieNode()
		trie.append(List[String]("San", "Francisco"))
		trie.append(List[String]("San", "Diego"))
		trie.append(List[String]("San", "to", "Domingo"))
		trie.append(List[String]("São", "Paulo"))

		val result = trie.findByPrefix(List[String]("san"))

		assert(result.length == 3)
		assert(result.contains(List[String]("San", "Francisco")))
		assert(result.contains(List[String]("San", "Diego")))
		assert(result.contains(List[String]("San", "to", "Domingo")))
	}

	"trie" should "not find by prefix if it does not exist" in {
		val trie = new TrieNode()
		trie.append(List[String]("São", "Paulo"))
		trie.append(List[String]("Rio", "de", "Janeiro"))
		trie.append(List[String]("Philadelphia"))

		assert(trie.findByPrefix(List[String]("bos")).isEmpty)
	}

	"trie" should "say that a word is contained" in {
		val trie = new TrieNode()
		trie.append(List[String]("João", "Pessoa"))

		assert(trie.contains(List[String]("João", "Pessoa")))
	}

	"trie" should "say that a word is not contained" in {
		val trie = new TrieNode()
		trie.append(List[String]("João", "Pessoa"))

		assert(!trie.contains(List[String]("João")))
	}

	"trie" should "provide the path to a word" in {
		val word = List[String]("San", "Diego")

		val trie = new TrieNode()
		trie.append(List[String]("San", "Francisco"))
		trie.append(word)

		val list = trie.pathTo(word).get
		assert(list.length == 3) // includes empty word
		(0 until 2).map { index =>
			assert(list(index).children.get(word(index).toLowerCase).isDefined)
		}
	}

	"trie" should "remove an item" in {
		val trie = new TrieNode()
		trie.append(List[String]("San", "Francisco"))
		trie.append(List[String]("San", "Antonio"))
		assert(trie.remove(List[String]("San", "Francisco")))
		assert(!trie.contains(List[String]("San", "Francisco")))
		assert(trie.contains(List[String]("San", "Antonio")))
	}

	"trie" should "remove a longer item" in {
		val name = List[String]("João", "Pessoa")

		val trie = new TrieNode()
		trie.append(List[String]("João"))
		trie.append(name)

		assert(trie.remove(name))

		assert(!trie.contains(name))
		assert(trie.contains(List[String]("João")))
	}

	"trie" should "remove a smaller item" in {
		val name = List[String]("João")

		val trie = new TrieNode()
		trie.append(List[String]("João", "Pessoa"))
		trie.append(name)

		assert(trie.remove(name))

		assert(!trie.contains(name))
		assert(trie.contains(List[String]("João", "Pessoa")))
	}

	"trie" should "not remove if it does not exist" in {
		val trie = new TrieNode()
		trie.append(List[String]("New", "York"))

		assert(!trie.remove(List[String]("Berlin")))
	}

	"trie" should "not remove if it is not a word node" in {
		val trie = new TrieNode()
		trie.append(List[String]("New", "York"))

		assert(!trie.remove(List[String]("New")))
	}

}
