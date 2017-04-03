package de.hpi.ingestion.textmining

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import scala.collection.mutable

/**
  * This class is a Kryo Serializer for the Scala mutable.Map.
  */
class MutableMapSerializer() extends Serializer[mutable.Map[_, _]] {

	/**
	  * Deserializes a mutable Map given an input buffer to read from.
	  * @param kryo current Kryo instance
	  * @param input input buffer to read from
	  * @param typ class of the object we read
	  * @return the deserialized object
	  */
	override def read(
		kryo: Kryo,
		input: Input,
		typ: Class[mutable.Map[_, _]]
	): mutable.Map[_, _] =
	{
		val mapLength = input.readInt(true)
		val resultMap = mutable.Map[Any, Any]()
		if (mapLength != 0) {
			var i = 0
			while (i < mapLength) {
				resultMap(kryo.readClassAndObject(input)) = kryo.readClassAndObject(input)
				i += 1
			}
		}
		resultMap
	}

	/**
	  * Serializes a mutable Map to a given output buffer.
	  * @param kryo current Kryo instance
	  * @param output output buffer to write to
	  * @param collection mutable Map to serialize
	  */
	override def write(kryo: Kryo, output: Output, collection: mutable.Map[_, _]) = {
		val mapLength = collection.size
		output.writeInt(mapLength, true)
		if (mapLength != 0) {
			val mapIterator = collection.iterator
			while (mapIterator.hasNext) {
				val mapEntry = mapIterator.next
				kryo.writeClassAndObject(output, mapEntry._1)
				kryo.writeClassAndObject(output, mapEntry._2)
			}
		}
	}
}
