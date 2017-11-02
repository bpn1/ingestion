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
