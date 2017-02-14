import edu.stanford.nlp.simple.GermanSentence
import scala.collection.JavaConverters._

trait Tokenizer {
	def tokenize(x: String): List[String]
}

class WhitespaceTokenizer() extends Tokenizer {
	def tokenize(txt: String) = txt.split(" ").toList
}

class CoreNLPTokenizer() extends Tokenizer {
	def tokenize(txt: String) = new GermanSentence(txt).words().asScala.toList
}
