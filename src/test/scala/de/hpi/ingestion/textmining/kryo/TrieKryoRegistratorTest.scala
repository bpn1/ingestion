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
