/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import scala.collection.mutable.ListBuffer

/**
  * This class is a Kryo Serializer for Scala Options.
  * It can only handle Options of Strings or Lists.
  */
class OptionSerializer() extends Serializer[Option[_]] {

	/**
	  * Deserializes an Object given an input buffer to read from.
	  *
	  * @param kryo  current Kryo instance
	  * @param input input input buffer to read from
	  * @param typ   class of the object we read
	  * @return the deserialized object
	  */
	override def read(kryo: Kryo, input: Input, typ: Class[Option[_]]): Option[_] = {
		val mode = input.readInt(true)
		mode match {
			case 1 =>
				val in = kryo.readClassAndObject(input)
				Option(in)
			case 2 =>
				val len = input.readInt(true)
				val in = ListBuffer[Any]()
				for(i <- 0 until len) {
					in += kryo.readClassAndObject(input)
				}
				Option(in.toList)
			case 3 | _ => None
		}
	}

	/**
	  * Serializes a mutable Map to a given output buffer.
	  * Only Options of Strings and Lists can be serialized.
	  *
	  * @param kryo   current Kryo instance
	  * @param output output buffer to write to
	  * @param value  Option to serialize
	  */
	override def write(kryo: Kryo, output: Output, value: Option[_]) = {
		value match {
			case Some(a: String) =>
				output.writeInt(1, true)
				kryo.writeClassAndObject(output, a)
			case Some(a: List[_]) =>
				output.writeInt(2, true)
				val len = a.length
				output.writeInt(len, true)
				val it = a.iterator
				while(it.hasNext) {
					val t = it.next
					kryo.writeClassAndObject(output, t)
				}
			case None | _ => output.writeInt(3, true)
		}
	}
}
