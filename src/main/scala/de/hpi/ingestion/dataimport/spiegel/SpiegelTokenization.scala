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

import de.hpi.fgis.utils.text.Document
import de.hpi.fgis.utils.text.annotation.Token
import de.hpi.fgis.utils.text.tokenize.opennlp.MaximumEntropyTokenizer
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class SpiegelTokenization extends SparkJob {
    appName = "Spiegel Tokenization"
    configFile = "textmining.xml"

    var sentences: RDD[String] = _
    var tokens: RDD[String] = _

    /**
      * Loads the Spiegel Articles from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        sentences = sc.textFile("spiegel_sentences2").repartition(128)
    }

    /**
      * Saves the sentences to the HDFS.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        tokens.saveAsTextFile("spiegel_sentences2_tokenized")
    }

    /**
      * Splits the Spiegel Articles into sentences.
      * @param sc SparkContext to be used for the job
      */
    override def run(sc: SparkContext): Unit = {
        tokens = sentences.map { sentence =>
            val document = new Document(sentence)
            val tokenizer = new MaximumEntropyTokenizer
            tokenizer.annotate(document)
            document.get(Token).map(_.value).mkString("\t")
        }
    }
}
