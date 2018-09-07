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

package de.hpi.ingestion.implicits

import org.apache.spark.sql.{Dataset, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object DatasetImplicits {
    implicit class DatasetFunctions[X](dataset: Dataset[X]) {
        def collect[Y](
            f: PartialFunction[X, Y]
        ): Dataset[Y] = {
            dataset
                .filter(f.isDefinedAt _)
                .map(f)
        }

        def min()(implicit ord: Ordering[X]): X = {
            dataset.reduce(ord.min _)
        }

        def max()(implicit ord: Ordering[X]): X = {
            dataset.reduce(ord.max _)
        }

        def isEmpty(): Boolean = dataset.rdd.isEmpty()

        def saveToCassandra(keyspaceName: String, tableName: String): Unit = {
            dataset.rdd.saveToCassandra(keyspaceName, tableName)
        }
    }

    implicit class PairDatasetFunctions[K, V](dataset: Dataset[(K, V)]) {
        def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
            dataset
                .rdd
                .reduceByKey(func)
        }

        def reduceByKey(spark: SparkSession, func: (V, V) => V): Dataset[(K, V)] = {
            spark.createDataset(dataset.reduceByKey(func))
        }

        def cogroup[W](other: Dataset[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = {
            dataset
                .rdd
                .cogroup(other.rdd)
        }

        def cogroup[W](spark: SparkSession, other: Dataset[(K, W)]): Dataset[(K, (Iterable[V], Iterable[W]))] = {
            spark.createDataset(dataset.cogroup(other))
        }

        def keys: Dataset[K] = dataset.map(_._1)

        def values: Dataset[V] = dataset.map(_._2)
    }

    implicit class SparkSessionFunctions(spark: SparkSession) {
        def cassandraTable[X](keyspace: String, tableName: String): Dataset[X] = {
            val tableRDD = spark.sparkContext.cassandraTable[X](keyspace, tableName)
            spark.createDataset(tableRDD)
        }
    }
}
