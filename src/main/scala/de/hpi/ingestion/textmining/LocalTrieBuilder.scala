package de.hpi.ingestion.textmining

import java.io.{FileOutputStream, OutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator
import de.hpi.ingestion.textmining.models.TrieNode
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

import scala.io.{BufferedSource, Source}

/**
  * Creates a Trie of a given list of aliases and serializes it to the filesystem.
  */
object LocalTrieBuilder {
	val aliasFileName = "aliaslist"
	val trieFileName = "trie_full.bin"

	/**
	  * Creates Trie of the aliases in the given file and serializes it to a binary of the given name.
	  *
	  * @param aliasStream stream of the file containing the aliases with one alias per line
	  * @param trieStream  output stream of the binary file of the trie
	  */
	def serializeTrie(aliasStream: BufferedSource, trieStream: OutputStream): Unit = {
		val kryo = new Kryo()
		TrieKryoRegistrator.register(kryo)
		val tokenizer = IngestionTokenizer(false, false)
		val trie = new TrieNode()

		for(line <- aliasStream.getLines()) {
			val tokens = tokenizer.process(line)
			trie.append(tokens)
		}

		val trieBinary = new Output(trieStream)
		kryo.writeObject(trieBinary, trie)
		trieBinary.close()
	}

	// $COVERAGE-OFF$
	// recommended scala flags
	// -J-Xmx16g
	// -J-Xss1g
	// -classpath ingestion-assembly-1.0.jar de.hpi.ingestion.textmining.LocalTrieBuilder
	def main(args: Array[String]): Unit = {
		serializeTrie(Source.fromFile(aliasFileName), new FileOutputStream(trieFileName))
	}
	// $COVERAGE-ON$
}
