package de.hpi.ingestion.textmining.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.FieldSerializer
import de.hpi.ingestion.textmining.models.TrieNode
import org.apache.spark.serializer.KryoRegistrator

import scala.collection.mutable

/**
  * This class is a Kryo Registrator which registers the class TrieNode.
  */
class TrieKryoRegistrator extends KryoRegistrator {
	/**
	  * Registers the class TrieNode with the correct Serializers for each field.
	  *
	  * @param kryo current Kryo instance
	  */
	override def registerClasses(kryo: Kryo): Unit = {
		TrieKryoRegistrator.register(kryo)
	}
}

/**
  * Companion object for the TrieKryoRegister used to register the Trie for non Spark Kryo instances.
  */
object TrieKryoRegistrator {

	/**
	  * Registers the Trie serializer for the given Kryo instance.
	  * @param kryo kryo instance used to register the Trie
	  */
	def register(kryo: Kryo): Unit = {
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
	}
}
