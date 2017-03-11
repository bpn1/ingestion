// from https://github.com/mauricio/scala-sandbox/blob/master/src/main/scala/trie/Trie.scala

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Trie {
	def apply(): Trie = new TrieNode()
}

sealed trait Trie extends Traversable[List[String]] with Serializable {
	def append(key : List[String])
	def findByPrefix(prefix: List[String]): scala.collection.Seq[List[String]]
	def contains(word: List[String]): Boolean
	def remove(word : List[String]): Boolean

}

class TrieNode(
	val token : Option[String] = None,
	var word: Option[List[String]] = None) extends Trie
{

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
			items: ListBuffer[List[String]]): ListBuffer[List[String]] =
		{
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

	def pathTo(word : List[String]): Option[ListBuffer[TrieNode]] = {

		def helper(
			buffer : ListBuffer[TrieNode],
			currentIndex : Int,
			node : TrieNode): Option[ListBuffer[TrieNode]] =
		{
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

	def matchTokens(tokens: List[String]): ListBuffer[List[String]] = {
		@tailrec def helper(
			currentIndex: Int,
			node: TrieNode,
			items: ListBuffer[List[String]]
		): ListBuffer[List[String]] =
		{
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

	def printableString(level: Int): String = {
		def helper(
			currentIndex: Int,
			node: TrieNode,
			outputStringBuffer: ListBuffer[String]
		): Unit =
		{
			val nodeDesc = s"Trie(token=$token,word=$word)"
			val indent = "\t" * currentIndex
			outputStringBuffer += indent + nodeDesc
			for(child <- node.children) {
				helper(currentIndex + 1, child._2, outputStringBuffer)
			}
		}
		val output = ListBuffer[String]()
		helper(0, this, output)
		output.mkString("\n")
	}

}
