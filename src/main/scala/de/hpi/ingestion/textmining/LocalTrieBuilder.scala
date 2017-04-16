package de.hpi.ingestion.textmining

import java.io.FileOutputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.serializers.FieldSerializer
import de.hpi.ingestion.textmining.models.{ParsedWikipediaEntry, TrieNode}

import scala.io.Source

import scala.collection.mutable

object LocalTrieBuilder {

	def main(args: Array[String]): Unit = {
		val kryo = new Kryo
//		val registrator = new TrieKryoRegistrator
//		registrator.registerClasses(kryo)
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
		val trie = new TrieNode()
		val aliasFile = Source.fromFile("aliaslist")
		var counter = 0
		for(line <- aliasFile.getLines()) {
			trie.append(tokenizer.tokenize(line))
			counter += 1
		}
		println(s"finished building trie with $counter elements\nnow serializing")
		val trieBinary = new Output(new FileOutputStream("trie.bin"))
		kryo.writeObject(trieBinary, trie)
		println("finished serializing")
		trieBinary.close()

	}
}
