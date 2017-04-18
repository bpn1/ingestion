package de.hpi.ingestion.textmining.models

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

class PartitionedTrie {
	var keys: List[String] = defaultKeys()
	var trieMap: Map[String, TrieNode] = Map[String, TrieNode]()
	var fileMap: Map[String, FSDataInputStream] = Map[String, FSDataInputStream]()
	val defaultKey = "default"
	var fs: FileSystem

	def defaultKeys(): List[String] = {
		val upperCaseLetters = (65 to 90).map(_.toChar.toString).toList
		val lowerCaseLetters = (97 to 122).map(_.toChar.toString).toList
		val defaultKey = List(defaultKey)
		upperCaseLetters ++ lowerCaseLetters ++ defaultKey
	}

	def initializeFileSystem(sc: SparkContext): Unit = {
		fs = FileSystem.get(sc.hadoopConfiguration)
	}

	def keyForTokens(tokens: List[String]): String = {
		assert(tokens.nonEmpty)
		val char = tokens.head.charAt(0)
		if(char)
	}

	def matchTokens(tokens: List[String]): ListBuffer[List[String]] = {
		if(tokens.isEmpty) {

		}
	}

	def fileName(key: String): String = {
		s"trie_binaries/trie_$key.bin"
	}

	def openTrieBinaries(path: String): Unit = {
		fileMap = keys
			.map(key => (key, fileName(key)))
			.toMap
			.mapValues(new Path(_))
			.mapValues(fs.open)
			.map(identity)
	}

	def deserializeTries(kryo: Kryo): Unit = {
		trieMap = fileMap
			.mapValues { file =>
				val inputStream = new Input(file)
				val trie = kryo.readObject(inputStream, classOf[TrieNode])
				inputStream.close()
				trie
			}.map(identity)
	}

}
