package de.hpi.ingestion.textmining

import java.io.FileOutputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.serializers.FieldSerializer
import de.hpi.ingestion.textmining.models.TrieNode

import scala.io.Source

import scala.collection.mutable

object LocalTrieBuilder {
	val defaultKey = "default"

	def addTokensToTrie(tokens: List[String], trieMap: mutable.Map[String, TrieNode]): Unit = {
		if(tokens.nonEmpty) {
			val firstChar = tokens.head.charAt(0)
			if(firstChar.isLetter && firstChar <= 'z') {
				val key = firstChar.toString
				if(!trieMap.contains(key)) {
					trieMap(key) = new TrieNode()
				}
				trieMap(key).append(tokens)
			} else {
				trieMap(defaultKey).append(tokens)
			}
		}
	}

	def serializeTries(kryo: Kryo, trieMap: mutable.Map[String, TrieNode]): Unit = {
		trieMap.foreach { case (key, trie) =>
			val fileName = s"trie_$key.bin"
			val trieBinary = new Output(new FileOutputStream(fileName))
			println(s"serializing trie for key $key")
			kryo.writeObject(trieBinary, trie)
			println(s"finished serializing trie for key $key")
			trieBinary.close()
		}
	}

	def main(args: Array[String]): Unit = {
		val kryo = new Kryo
		kryo.register(classOf[mutable.Map[_, _]], new MutableMapSerializer())
		kryo.register(classOf[Option[_]], new OptionSerializer())

		val trieSerializer = new FieldSerializer(kryo, classOf[TrieNode])
		trieSerializer
			.getField("children")
			.setClass(
				classOf[mutable.Map[_, _]],
				kryo.getSerializer(classOf[mutable.Map[_, _]]))
		trieSerializer
			.getField("token")
			.setClass(
				classOf[Option[_]],
				kryo.getSerializer(classOf[Option[_]]))
		trieSerializer
			.getField("word")
			.setClass(
				classOf[Option[_]],
				kryo.getSerializer(classOf[Option[_]]))
		kryo.register(classOf[TrieNode], trieSerializer)

		val tokenizer = new CoreNLPTokenizer
		val aliasFile = Source.fromFile("aliaslist")
		var counter = 0
		val trieMap = mutable.Map[String, TrieNode]()
		trieMap(defaultKey) = new TrieNode()
		for(line <- aliasFile.getLines()) {
			val tokens = tokenizer.tokenize(line)
			addTokensToTrie(tokens, trieMap)
			counter += 1
		}
		println(s"serializing ${trieMap.keySet.size} tries for $counter aliases")
		serializeTries(kryo, trieMap)
		println(s"finished serializing tries")
	}
}
