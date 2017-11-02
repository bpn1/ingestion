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

import java.io.InputStream
import com.datastax.spark.connector._
import com.esotericsoftware.kryo.io.Input
import com.twitter.chill.Kryo
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.nel.ArticleTrieSearch
import scala.collection.mutable.ListBuffer

/**
  * Finds all occurrences of the aliases in the given trie in all Wikipedia articles and writes them to the
  * `foundaliases` column.
  * recommended spark-submit flags (needed for deserialization of Trie)
  * --conf "spark.executor.extraJavaOptions=-XX:ThreadStackSize=1000000"
  */
class AliasTrieSearch extends SparkJob {
    import AliasTrieSearch._
    appName = "Alias Trie Search"
    configFile = "textmining.xml"
    sparkOptions("spark.kryo.registrator") = "de.hpi.ingestion.textmining.kryo.TrieKryoRegistrator"

    var parsedWikipedia: RDD[ParsedWikipediaEntry] = _
    var parsedWikipediaWithAliases: RDD[ParsedWikipediaEntry] = _

    // $COVERAGE-OFF$
    var trieStreamFunction: String => InputStream = hdfsFileStream
    /**
      * Loads Parsed Wikipedia entries from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        parsedWikipedia = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
    }

    /**
      * Saves Parsed Wikipedia entries with the found aliases to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        parsedWikipediaWithAliases.saveToCassandra(settings("keyspace"), settings("parsedWikiTable"))
    }
    // $COVERAGE-ON$

    /**
      * Uses a pre-built Trie to find aliases in the text of articles and writes them into the foundaliases field.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val tokenizer = IngestionTokenizer(false, false)
        parsedWikipediaWithAliases = parsedWikipedia.mapPartitions({ partition =>
            val trie = deserializeTrie(trieStreamFunction(settings("trieFile")))
            partition.map(matchEntry(_, trie, tokenizer))
        }, true)
    }
}

object AliasTrieSearch {
    // $COVERAGE-OFF$
    /**
      * Opens a HDFS file stream pointing to the trie binary.
      *
      * @return Input Stream pointing to the file in the HDFS
      */
    def hdfsFileStream(file: String): InputStream = {
        val hadoopConf = new Configuration()
        val fs = FileSystem.get(hadoopConf)
        fs.open(new Path(file))
    }
    // $COVERAGE-ON$

    /**
      * Deserializes Trie binary into a TrieNode.
      *
      * @param trieStream Input Stream pointing to the Trie binary data
      * @return deserialized Trie
      */
    def deserializeTrie(trieStream: InputStream): TrieNode = {
        val kryo = new Kryo()
        TrieKryoRegistrator.register(kryo)
        val inputStream = new Input(trieStream)
        val trie = kryo.readObject(inputStream, classOf[TrieNode])
        inputStream.close()
        trie
    }

    /**
      * Finds all occurrences of aliases in the text of a Wikipedia entry. All found aliases are saved in the
      * foundalias column and the longest match of each alias is saved with its offset and context in the triealiases
      * column. These triealiases are only those which are not an extended link.
      *
      * @param entry     parsed Wikipedia entry to use
      * @param trie      Trie containing the aliases we look for
      * @param tokenizer Tokenizer used to tokenize the text of the entry
      * @return entry containing list of found aliases
      */
    def matchEntry(
        entry: ParsedWikipediaEntry,
        trie: TrieNode,
        tokenizer: IngestionTokenizer
    ): ParsedWikipediaEntry = {
        val trieAliasArticle = TrieAliasArticle(entry.title, text = entry.text)
        val trieAliases = ArticleTrieSearch
            .findAliases(trieAliasArticle, tokenizer, trie)
            .triealiases
            .filterNot { alias =>
                val isLink = entry.textlinks.exists(_.offset == alias.offset)
                val isExLink = entry.extendedlinks().exists(_.offset == alias.offset)
                val isStopword = tokenizer.stopwords.contains(alias.alias)
                val isSymbol = Set(".", "!", "?", ",", ";", ":", "(", ")", "*", "#", "+").contains(alias.alias)
                isLink || isExLink || isStopword || isSymbol
            }
        val resultList = ListBuffer[List[OffsetToken]]()
        var contextAliases = List[TrieAlias]()
        val tokens = tokenizer.processWithOffsets(entry.getText())
        for(i <- tokens.indices) {
            val testTokens = tokens.slice(i, tokens.length)
            val aliasMatches = trie.matchTokens(testTokens.map(_.token))
            val offsetMatches = aliasMatches.map(textTokens => testTokens.take(textTokens.length))
            resultList ++= offsetMatches
            offsetMatches.sortBy(_.length).lastOption.foreach { longestMatch =>
                val offset = longestMatch.headOption.map(_.beginOffset)
                offset.foreach { begin =>
                    val alias = entry.getText().substring(begin, longestMatch.last.endOffset)
                    contextAliases :+= TrieAlias(alias, offset)
                }
            }
        }
        val foundAliases = resultList
            .filter(_.nonEmpty)
            .map { offsetTokens =>
                val begin = offsetTokens.headOption.map(_.beginOffset)
                val end = offsetTokens.lastOption.map(_.endOffset)
                (begin, end)
            }.collect {
            case (begin, end) if begin.isDefined && end.isDefined =>
                entry.getText().substring(begin.get, end.get)
        }.toList
        entry.copy(foundaliases = cleanFoundAliases(foundAliases), triealiases = trieAliases)
    }

    /**
      * Removes empty aliases from the given list of aliases.
      *
      * @param aliases List of aliases found in an article
      * @return cleaned List of aliases
      */
    def cleanFoundAliases(aliases: List[String]): List[String] = {
        aliases.filter(_.nonEmpty)
    }
}
