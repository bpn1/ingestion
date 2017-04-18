package de.hpi.ingestion.textmining.models

import de.hpi.ingestion.implicits.CharImplicits._
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.fs.FSDataInputStream

import scala.collection.mutable.ListBuffer

class PartitionedTrie(keys: List[String]) extends Serializable {
	val defaultKey = "default"
	var trieMap: Map[String, TrieNode] = Map[String, TrieNode]()

	def keyForTokens(tokens: List[String]): String = {
		tokens.headOption
			.filter(_.nonEmpty)
			.map(_.charAt(0))
			.filter(_.isASCIILetter)
			.map(_.toString)
			.getOrElse(defaultKey)
	}

	def matchTokens(tokens: List[String]): ListBuffer[List[String]] = {
		val key = keyForTokens(tokens)
		assert(trieMap.contains(key))
		trieMap(key).matchTokens(tokens)
	}

	def deserializeTries(kryo: Kryo, fileMap: Map[String, FSDataInputStream]): Unit = {
		trieMap = fileMap
			.mapValues { file =>
				val inputStream = new Input(file)
				val trie = kryo.readObject(inputStream, classOf[TrieNode])
				inputStream.close()
				trie
			}.map(identity)
	}

}

object PartitionedTrie {
	def apply(keys: List[String]) = new PartitionedTrie(keys)
}
