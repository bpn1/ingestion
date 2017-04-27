package de.hpi.ingestion.textmining.models

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Represents a Trie made up of TrieNodes.
  * Source: https://git.io/v9sVB
  */
object Trie {
	def apply(): Trie = new TrieNode()
}

/**
  * Trait declaring the methods needed for using the Trie.
  */
sealed trait Trie extends Traversable[List[String]] with Serializable {
	/**
	  * Appends a list of tokens to the trie.
	  * @param key List of tokens representing a word.
	  */
	def append(key : List[String])

	/**
	  * Finds all words with a given prefix.
	  * @param prefix List of tokens used as prefix in the search
	  * @return Sequence of lists of tokens starting with the prefix
	  */
	def findByPrefix(prefix: List[String]): Seq[List[String]]

	/**
	  * Checks if a given word is part of this trie.
	  * @param word List of tokens to be searched for
	  * @return true of the given word is element of the trie
	  */
	def contains(word: List[String]): Boolean

	/**
	  * Removes a given word from the trie.
	  * @param word List of tokens to be removed
	  * @return true if the word was element of the trie and was removed and otherwise false
	  */
	def remove(word : List[String]): Boolean

	/**
	  * Returns path of Trie Nodes to a given word if it exists and otherwise None.
	  * @param word list of tokens to be searched for
	  * @return List of Trie Nodes representing the path for the given word
	  */
	def pathTo(word : List[String]): Option[ListBuffer[TrieNode]]

	/**
	  * Matches a List of tokens against this trie and returns a List of all found words.
	  * @param tokens List of tokens containing possible words
	  * @return List of found words represented as list of tokens
	  */
	def matchTokens(tokens: List[String]): ListBuffer[List[String]]
}

/**
  * A node of a Trie containing a token which might be the empty word and a possible word that ends on this Node.
  * @param token token this Trie Node represents
  * @param word possible word ending on this node
  */
class TrieNode(val token : Option[String] = None, var word: Option[List[String]] = None) extends Trie {

	def this() = this(None, None)

	val children: mutable.Map[String, TrieNode] = new java.util.TreeMap[String, TrieNode]().asScala

	override def append(key: List[String]) = {
		@tailrec def appendHelper(node: TrieNode, currentIndex: Int): Unit = {
			if (currentIndex == key.length) {
				node.word = Some(key)
			} else {
				val token = key(currentIndex)
				val result = node.children.getOrElseUpdate(token, {
					new TrieNode(Some(token))
				})
				appendHelper(result, currentIndex + 1)
			}
		}

		appendHelper(this, 0)
	}

	override def foreach[U](f: List[String] => U): Unit = {
		@tailrec def foreachHelper(nodes: TrieNode*): Unit = {
			if (nodes.nonEmpty) {
				nodes.foreach(node => node.word.foreach(f))
				foreachHelper(nodes.flatMap(node => node.children.values): _*)
			}
		}

		foreachHelper(this)
	}

	override def findByPrefix(prefix: List[String]): scala.collection.Seq[List[String]] = {
		@tailrec def helper(
			currentIndex: Int,
			node: TrieNode,
			items: ListBuffer[List[String]]
		): ListBuffer[List[String]] = {
			if (currentIndex == prefix.length) {
				items ++ node
			} else {
				node.children.get(prefix(currentIndex)) match {
					case Some(child) => helper(currentIndex + 1, child, items)
					case None => items
				}
			}
		}

		helper(0, this, new ListBuffer[List[String]]())
	}

	override def contains(word: List[String]): Boolean = {
		@tailrec def helper(currentIndex: Int, node: TrieNode): Boolean = {
			if (currentIndex == word.length) {
				node.word.isDefined
			} else {
				node.children.get(word(currentIndex)) match {
					case Some(child) => helper(currentIndex + 1, child)
					case None => false
				}
			}
		}

		helper(0, this)
	}

	override def remove(word : List[String]): Boolean = {
		pathTo(word) match {
			case Some(path) => {
				var index = path.length - 1
				var continue = true
				path(index).word = None

				while (index > 0 && continue) {
					val current = path(index)
					if (current.word.isDefined) {
						continue = false
					} else {
						val parent = path(index - 1)
						if (current.children.isEmpty) {
							parent.children.remove(word(index - 1))
						}
						index -= 1
					}
				}
				true
			}
			case None => false
		}
	}

	override def pathTo(word : List[String]): Option[ListBuffer[TrieNode]] = {
		def helper(
			buffer : ListBuffer[TrieNode],
			currentIndex : Int,
			node : TrieNode
		): Option[ListBuffer[TrieNode]] = {
			if (currentIndex == word.length) {
				node.word.map(word => buffer += node)
			} else {
				node.children.get(word(currentIndex)) match {
					case Some(found) => {
						buffer += node
						helper(buffer, currentIndex + 1, found)
					}
					case None => None
				}
			}
		}

		helper(new ListBuffer[TrieNode](), 0, this)
	}

	override def matchTokens(tokens: List[String]): ListBuffer[List[String]] = {
		@tailrec def helper(
			currentIndex: Int,
			node: TrieNode,
			items: ListBuffer[List[String]]
		): ListBuffer[List[String]] = {
			if(node.word.isDefined) {
				items += node.word.get
			}
			if(currentIndex >= tokens.length) {
				return items
			}
			node.children.get(tokens(currentIndex)) match {
				case Some(child) => helper(currentIndex + 1, child, items)
				case None => items
			}
		}

		helper(0, this, new ListBuffer[List[String]]())
	}

	override def toString(): String = s"Trie(token=$token,word=$word)"

	/**
	  * Returns if this object can be compared with another object.
	  * @param other object this object should be compared with
	  * @return true if the other object is also a TrieNode
	  */
	def canEqual(other: Any): Boolean = other.isInstanceOf[TrieNode]

	override def equals(other: Any): Boolean = other match {
		case that: TrieNode =>
			(that canEqual this) &&
				children == that.children &&
				token == that.token &&
				word == that.word
		case _ => false
	}

	override def hashCode(): Int = {
		val state = Seq(children, token, word)
		state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
	}
}
