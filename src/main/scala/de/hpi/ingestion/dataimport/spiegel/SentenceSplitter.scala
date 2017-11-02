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

package de.hpi.ingestion.dataimport.spiegel

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models.TrieAliasArticle
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.fgis.utils.text.Document
import de.hpi.fgis.utils.text.annotation.Sentence
import de.hpi.fgis.utils.text.sentencesplit.MEOpenNLPSentenceSplitter

/**
  * Splits the Spiegel articles into sentences using a self trained Maximum Entropy model.
  */
class SentenceSplitter extends SparkJob {
	appName = "Sentence Splitter"
	configFile = "textmining.xml"

	var spiegelArticles: RDD[TrieAliasArticle] = _
	var sentences: RDD[String] = _

	/**
	  * Loads the Spiegel Articles from the Cassandra.
	  * @param sc SparkContext to be used for the job
	  */
	override def load(sc: SparkContext): Unit = {
		spiegelArticles = sc.cassandraTable[TrieAliasArticle](settings("keyspace"), settings("spiegelTable"))
	}

	/**
	  * Saves the sentences to the HDFS.
	  * @param sc SparkContext to be used for the job
	  */
	override def save(sc: SparkContext): Unit = {
		sentences.saveAsTextFile("spiegel_sentences2")
	}

	/**
	  * Splits the Spiegel Articles into sentences.
	  * @param sc SparkContext to be used for the job
	  */
	override def run(sc: SparkContext): Unit = {
		sentences = spiegelArticles.map { article =>
			val document = new Document(article.text.get)
			val sentenceSplitter = new MEOpenNLPSentenceSplitter
			sentenceSplitter.annotate(document)
			document.get(Sentence).map(_.value).mkString("\n")
		}
	}
}
