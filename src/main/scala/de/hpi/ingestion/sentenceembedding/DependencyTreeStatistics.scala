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
import de.hpi.ingestion.sentenceembedding.models.{DependencyTree, DependencyTreePattern}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Aggregates the patterns of dependency parse trees, saves the counts to the sentences and saves the aggregated
  * sentences of every pattern to the Cassandra.
  */
class DependencyTreeStatistics extends SparkJob {
    appName = "Dependency Tree Statistics"
    configFile = "sentenceembeddings.xml"

    var dependencyTrees: RDD[DependencyTree] = _
    var patternFrequencies: RDD[(Long, Int)] = _
    var aggregatedSentences: RDD[DependencyTreePattern] = _

    // $COVERAGE-OFF$
    /**
      * Loads the sentences and their dependency parse trees from the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def load(sc: SparkContext): Unit = {
        dependencyTrees = sc.cassandraTable[DependencyTree](settings("keyspace"), settings("depTreeTable"))
    }

    /**
      * Saves the pattern frequencies and the aggregated sentences to the Cassandra.
      * @param sc SparkContext to be used for the job
      */
    override def save(sc: SparkContext): Unit = {
        patternFrequencies.saveToCassandra(
            settings("keyspace"),
            settings("depTreeTable"),
            SomeColumns("id", "patternfrequency"))
        aggregatedSentences.saveToCassandra(settings("keyspace"), settings("dptPatternTable"))
    }
    // $COVERAGE-ON$

    /**
      * Counts the number of occurrences of every pattern and aggregates the sentences by their patterns.
      * @param sc SparkContext to be used for the job
      */
    override def run(sc: SparkContext): Unit = {
        patternFrequencies = dependencyTrees
            .map(tree => (tree.pattern, List(tree.id)))
            .reduceByKey(_ ++ _)
            .flatMap { case (pattern, sentenceIds) =>
                val frequency = sentenceIds.length
                sentenceIds.map((_, frequency))
            }
        val frequencies = dependencyTrees
            .flatMap(tree => tree.pattern.map((_, List(tree.id))))
            .reduceByKey(_ ++ _)
            .flatMap { case (pattern, sentenceIds) =>
                val frequency = sentenceIds.length
                sentenceIds.map((_, (pattern, frequency)))
            }
        val sentences = dependencyTrees.flatMap(tree => tree.sentence.map((tree.id, _)))
        aggregatedSentences = frequencies
            .join(sentences)
            .map { case (id, ((pattern, frequency), sentence)) =>
                (DependencyTreePattern(pattern, Option(frequency)), List(sentence))
            }.reduceByKey(_ ++ _)
            .sortBy(-_._1.frequency.get)
            .map { case (dptPattern, sentenceList) => dptPattern.copy(sentences = sentenceList) }
    }
}
