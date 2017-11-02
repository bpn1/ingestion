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
import scala.collection.mutable

/**
  * This class is a Kryo Serializer for the Scala mutable.Map.
  */
class MutableMapSerializer() extends Serializer[mutable.Map[_, _]] {

    /**
      * Deserializes a mutable Map given an input buffer to read from.
      *
      * @param kryo  current Kryo instance
      * @param input input buffer to read from
      * @param typ   class of the object we read
      * @return the deserialized object
      */
    override def read(
        kryo: Kryo,
        input: Input,
        typ: Class[mutable.Map[_, _]]
    ): mutable.Map[_, _] = {
        val mapLength = input.readInt(true)
        val resultMap = mutable.Map[Any, Any]()
        if(mapLength != 0) {
            var i = 0
            while(i < mapLength) {
                resultMap(kryo.readClassAndObject(input)) = kryo.readClassAndObject(input)
                i += 1
            }
        }
        resultMap
    }

    /**
      * Serializes a mutable Map to a given output buffer.
      *
      * @param kryo       current Kryo instance
      * @param output     output buffer to write to
      * @param collection mutable Map to serialize
      */
    override def write(kryo: Kryo, output: Output, collection: mutable.Map[_, _]) = {
        val mapLength = collection.size
        output.writeInt(mapLength, true)
        if(mapLength != 0) {
            val mapIterator = collection.iterator
            while(mapIterator.hasNext) {
                val mapEntry = mapIterator.next
                kryo.writeClassAndObject(output, mapEntry._1)
                kryo.writeClassAndObject(output, mapEntry._2)
            }
        }
    }
}
