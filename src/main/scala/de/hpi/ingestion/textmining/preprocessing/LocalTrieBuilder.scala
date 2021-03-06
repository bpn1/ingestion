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

package de.hpi.ingestion.textmining.preprocessing

import java.io.{FileOutputStream, OutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator
import de.hpi.ingestion.textmining.models.TrieNode
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer

import scala.io.{BufferedSource, Source}

/**
  * Creates a token based trie of a given list of aliases and serializes it to the filesystem.
  */
object LocalTrieBuilder {
    val aliasFileName = "aliaslist"
    val trieFileName = "trie_full.bin"

    /**
      * Creates Trie of the aliases in the given file and serializes it to a binary of the given name.
      *
      * @param aliasStream stream of the file containing the aliases with one alias per line
      * @param trieStream  output stream of the binary file of the trie
      */
    def serializeTrie(aliasStream: BufferedSource, trieStream: OutputStream): Unit = {
        val kryo = new Kryo()
        TrieKryoRegistrator.register(kryo)
        val tokenizer = IngestionTokenizer(false, false)
        val trie = new TrieNode()

        for(line <- aliasStream.getLines()) {
            val tokens = tokenizer.process(line)
            trie.append(tokens)
        }

        val trieBinary = new Output(trieStream)
        kryo.writeObject(trieBinary, trie)
        trieBinary.close()
    }

    // $COVERAGE-OFF$
    // recommended scala flags
    // -J-Xmx16g
    // -J-Xss1g
    // -classpath ingestion-assembly-1.0.jar de.hpi.ingestion.textmining.LocalTrieBuilder
    def main(args: Array[String]): Unit = {
        serializeTrie(Source.fromFile(aliasFileName), new FileOutputStream(trieFileName))
    }
    // $COVERAGE-ON$
}
