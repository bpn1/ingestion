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

package de.hpi.ingestion.sentenceembedding

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.sentenceembedding.models.DependencyTree
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

class DependencyTreeStatistics extends SparkJob {
    appName = "Dependency Tree Statistics"
    configFile = "sentenceembeddings.xml"

    var dependencyTrees: RDD[DependencyTree] = _
    var patternFrequencies: RDD[(Long, Int)] = _
    var patternWithSentences: RDD[String] = _

    override def load(sc: SparkContext): Unit = {
        dependencyTrees = sc.cassandraTable[DependencyTree](settings("keyspace"), settings("depTreeTable"))
    }

    override def save(sc: SparkContext): Unit = {
//        patternFrequencies.saveToCassandra(
//            settings("keyspace"),
//            settings("depTreeTable"),
//            SomeColumns("id", "patternfrequency"))
        patternWithSentences.saveAsTextFile(s"pattern_with_sentences_${System.currentTimeMillis()}")
    }

    override def run(sc: SparkContext): Unit = {
//        patternFrequencies = dependencyTrees
//            .map(tree => (tree.pattern, List(tree.id)))
//            .reduceByKey(_ ++ _)
//            .flatMap { case (pattern, sentenceIds) =>
//                val frequency = sentenceIds.length
//                sentenceIds.map((_, frequency))
//            }
        val frequencies = dependencyTrees
            .flatMap(tree => tree.pattern.map((_, List(tree.id))))
            .reduceByKey(_ ++ _)
            .flatMap { case (pattern, sentenceIds) =>
                val frequency = sentenceIds.length
                sentenceIds.map((_, (pattern, frequency)))
            }
        val sentences = dependencyTrees.flatMap(tree => tree.sentence.map((tree.id, _)))
        patternWithSentences = frequencies
            .join(sentences)
            .map { case (id, ((pattern, frequency), sentence)) =>
                ((pattern, frequency), List((id, sentence)))
            }.reduceByKey(_ ++ _)
            .sortBy(-_._1._2)
            .map { case ((pattern, frequency), sentenceList) =>
                val joinedSentences = sentenceList.map { case (id, sentence) => s"$id\t$sentence" }.mkString("\n")
                s"$pattern\t$frequency\n$joinedSentences"
            }
    }
}
