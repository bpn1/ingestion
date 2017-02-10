import edu.stanford.nlp.simple.GermanSentence
import scala.collection.JavaConverters._

trait Tokenizer extends Serializable {
	def tokenize(x: String): List[String]
	def reverse(x: List[String]): String
}

class WhitespaceTokenizer() extends Tokenizer {
	def tokenize(txt: String) = txt.split(" ").toList
	def reverse(tokens: List[String]) = tokens.mkString(" ")
}

class CoreNLPTokenizer() extends Tokenizer {
	def tokenize(txt: String) = new GermanSentence(txt).words().asScala.toList
	def reverse(tokens: List[String]) = tokens.mkString(" ")
}

class SentenceTokenizer() extends Tokenizer {
	def tokenize(txt: String) = txt.split(".").toList
	def reverse(tokens: List[String]) = tokens.mkString(".")
}
