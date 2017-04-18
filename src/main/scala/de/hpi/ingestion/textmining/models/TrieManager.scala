package de.hpi.ingestion.textmining.models

import com.twitter.chill.Kryo
import de.hpi.ingestion.implicits.CharImplicits._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.SparkContext

class TrieManager extends Serializable {
	val defaultKey = "default"
	var keys: List[String] = Nil
	var fileMap: Map[String, FSDataInputStream] = Map[String, FSDataInputStream]()
	var fs: FileSystem = null

	def defaultKeys(): List[String] = {
		val upperCaseLetters = (65 to 90).map(_.toChar.toString).toList
		val lowerCaseLetters = (97 to 122).map(_.toChar.toString).toList
		upperCaseLetters ++ lowerCaseLetters ++ List(defaultKey)
	}

	def initializeFileSystem(sc: SparkContext): Unit = {
		fs = FileSystem.get(sc.hadoopConfiguration)
	}

	def initializeKeys(): Unit = {
		keys = defaultKeys()
	}

	def keyForTokens(tokens: List[String]): String = {
		tokens.headOption
			.filter(_.nonEmpty)
			.map(_.charAt(0))
			.filter(_.isASCIILetter)
			.map(_.toString)
			.getOrElse(defaultKey)
	}

	def fileName(path: String, key: String): String = {
		s"${path}trie_$key.bin"
	}

	def openTrieBinaries(path: String): Unit = {
		fileMap = keys
			.map(key => (key, fileName(path, key)))
			.toMap
			.mapValues(new Path(_))
			.mapValues(fs.open)
			.map(identity)
	}

	def partitionedTrie(kryo: Kryo): PartitionedTrie = {
		val trie = PartitionedTrie(keys)
		trie.deserializeTries(kryo, fileMap)
		trie
	}
}
