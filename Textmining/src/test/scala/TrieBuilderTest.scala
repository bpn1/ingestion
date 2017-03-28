import WikiClasses.{ParsedWikipediaEntry, Link}
import org.scalatest.FlatSpec

class TrieBuilderTest extends FlatSpec {

	"TrieBuilder" should "find matches" in {
		val resultEntry = testEntryFoundAliases()
		assert(resultEntry.foundaliases.nonEmpty)
	}

	it should "not create duplicate entries" in {
		val resultEntry = testEntryFoundAliases()
		assert(resultEntry.foundaliases.size == resultEntry.foundaliases.distinct.size)
	}

	it should "not find empty string matches" in {
		val resultEntry = testEntryFoundAliases()
		assert(!resultEntry.foundaliases.contains(""))
	}

	def testEntryFoundAliases(): ParsedWikipediaEntry = {
		val entry = TestData.parsedTestEntry()
		val trie = new TrieNode()
		val tokenizer = new WhitespaceTokenizer()
		for(link <- entry.allLinks) {
			trie.append(tokenizer.tokenize(link.alias))
		}
		TrieBuilder.matchEntry(entry, trie, tokenizer)
	}
}
