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
import de.hpi.ingestion.textmining.models.TrieNode
import org.scalatest.{FlatSpec, Matchers}

class TrieKryoRegistratorTest extends FlatSpec with Matchers {

	"Trie Node" should "be registered by the object" in {
		var kryo = new Kryo()
		val unregisteredId = kryo.getRegistration(classOf[TrieNode]).getId
		unregisteredId should be < 0

		kryo = new Kryo()
		TrieKryoRegistrator.register(kryo)
		val registeredId = kryo.getRegistration(classOf[TrieNode]).getId
		registeredId should be > 0
	}

	it should "be registered by the class" in {
		var kryo = new Kryo()
		val registrator = new TrieKryoRegistrator
		val unregisteredId = kryo.getRegistration(classOf[TrieNode]).getId
		unregisteredId should be < 0

		kryo = new Kryo()
		registrator.registerClasses(kryo)
		val registeredId = kryo.getRegistration(classOf[TrieNode]).getId
		registeredId should be > 0
	}
}
