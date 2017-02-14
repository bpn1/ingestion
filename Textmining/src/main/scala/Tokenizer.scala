import java.util.StringTokenizer
import edu.stanford.nlp.simple.{GermanDocument, GermanSentence}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

trait Tokenizer extends Serializable {
	def tokenize(x: String): List[String]

	def reverse(x: List[String]): String
}

class WhitespaceTokenizer() extends Tokenizer {
	def tokenize(txt: String) = txt.split(" ").toList

	def reverse(tokens: List[String]) = tokens.mkString(" ")
}

class CleanWhitespaceTokenizer() extends Tokenizer {
	def stripAll(s: String, bad: String): String = {
		// http://stackoverflow.com/questions/17995260/trimming-strings-in-scala
		@scala.annotation.tailrec def start(n: Int): String =
		if (n == s.length) ""
		else if (bad.indexOf(s.charAt(n)) < 0) end(n, s.length)
		else start(1 + n)

		@scala.annotation.tailrec def end(a: Int, n: Int): String =
			if (n <= a) s.substring(a, n)
			else if (bad.indexOf(s.charAt(n - 1)) < 0) s.substring(a, n)
			else end(a, n - 1)

		start(0)
	}

	def tokenize(txt: String) = {
		val delimiters = " \n"
		val badCharacters = "().!?,;:'`\"„“"
		val stringTokenizer = new StringTokenizer(txt, delimiters)
		val tokens = new ListBuffer[String]()

		while (stringTokenizer.hasMoreTokens)
			tokens += stringTokenizer.nextToken()

		tokens
			.map(token => stripAll(token, badCharacters))
			.toList
	}

	def reverse(tokens: List[String]) = tokens.mkString(" ")
}

class CleanCoreNLPTokenizer() extends CoreNLPTokenizer {
	override def tokenize(txt: String) = {
		val tokens = super.tokenize(txt)
		val badTokens = Set[String](".", "!", "?", ",", ";", ":")
		tokens.filter(token => !badTokens.contains(token))
	}
}

class CoreNLPTokenizer() extends Tokenizer {
	def tokenize(txt: String) = {
		new GermanDocument(txt)
			.sentences
			.asScala
			.toList
			.flatMap(sentence => sentence.words.asScala.toList)
	}

	def reverse(tokens: List[String]) = tokens.mkString(" ")
}

class SentenceTokenizer() extends Tokenizer {
	def tokenize(txt: String) = txt.split(".").toList

	def reverse(tokens: List[String]) = tokens.mkString(".")
}
