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

package de.hpi.ingestion.dataimport.wikidata

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import scala.util.matching.Regex
import de.hpi.ingestion.dataimport.wikidata.models.WikidataEntity
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD

/**
  * Resolves the Wikidata IDs in the properties of each `WikidataEntity`. Entities labeled with an instancetype in the
  * `TagEntities` job are not resolved.
  */
class ResolveEntities extends SparkJob {
    import ResolveEntities._
    appName = "ResolveEntities"
    configFile = "wikidata_import.xml"

    var wikidataEntities: RDD[WikidataEntity] = _
    var resolvedIds: RDD[(String, Map[String, List[String]])] = _

    // $COVERAGE-OFF$
    /**
      * Loads the Wikidata entities from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        wikidataEntities = sc.cassandraTable[WikidataEntity](settings("keyspace"), settings("wikidataTable"))
    }

    /**
      * Saves the Wikidata entities with resolved ids to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        resolvedIds.saveToCassandra(settings("keyspace"), settings("wikidataTable"), SomeColumns("id", "data"))
    }
    // $COVERAGE-ON$

    /**
      * Resolves the Wikidata id of untagged entities in the properties of every Wikidata Entity.
      * @param sc Spark Context used to e.g. broadcast variables
      * @return List of RDDs containing the output data
      */
    override def run(sc: SparkContext): Unit = {
        val entityData = wikidataEntities.flatMap(flattenWikidataEntity).cache
        val noIdData = entityData.filter(!containsWikidataIdValue(_))
        val noEntityData = noIdData.filter(!hasUnitValue(_))
        val unitData = noIdData
            .filter(hasUnitValue)
            .map(splitUnitValue)
        val resolvableNames = wikidataEntities
            .filter(shouldBeResolved)
            .map(extractNameData)

        val idData = entityData
            .filter(containsWikidataIdValue)
            .map(makeJoinable)

        val idJoin = joinIdRDD(idData, resolvableNames)
        val unitJoin = joinUnitRDD(unitData, resolvableNames)

        // concatenate all RDDs
        val resolvedData = idJoin
            .union(unitJoin)
            .union(noEntityData)

        resolvedIds = rebuildProperties(resolvedData)
    }
}

object ResolveEntities {
    /**
      * Flattens Wikidata entitiy into triples of entity id, property key and property value.
      * @param entity Wikidata entity to flatten
      * @return triple of entity id, property key and property value
      */
    def flattenWikidataEntity(entity: WikidataEntity): List[(String, String, String)] = {
        entity.data.flatMap { case (property, valueList) =>
            valueList.map(element => (entity.id, property, element))
        }.toList
    }

    /**
      * Returns true if the third tuple element (the property value) is a Wikidata id.
      * @param element triple of entity id, property key, property value
      * @return true if property value is a wikidata id
      */
    def containsWikidataIdValue(element: (String, String, String)): Boolean = {
        val entityRegex = new Regex("^(P|Q)[0-9]+$")
        entityRegex.findFirstIn(element._3).isDefined
    }

    /**
      * Returns true if the entry does contains a unit of measurement reference in its value.
      * @param element triple of entity id, property key, property value
      * @return true if the property value does ends with a Wikidata id
      */
    def hasUnitValue(element: (String, String, String)): Boolean = {
        val unitRegex = new Regex(";(P|Q)[0-9]+$")
        unitRegex.findFirstIn(element._3).isDefined
    }

    /**
      * Splits entry with unit id as value into joinable tuple with the unit id as key and the
      * entry without unit id as value.
      * @param element triple of entity id, property key, property value
      * @return tuple of unit id and triple of entity id, property key, property value
      */
    def splitUnitValue(element: (String, String, String)): (String, (String, String, String)) = {
        val valueList = element._3.split(";")
        (valueList(1), (element._1, element._2, valueList(0)))
    }

    /**
      * Returns true if the entity id should be resolved in the properties of other entities.
      * @param entity entity to resolve
      * @return true if the entity has a label and no instance type
      */
    def shouldBeResolved(entity: WikidataEntity): Boolean = {
        entity.label.isDefined && entity.instancetype.isEmpty
    }

    /**
      * Extracts entity id and name of Wikidata entity in a joinable format.
      * @param entity Wikidata entity to use
      * @return tuple of wikidata id and entity label
      */
    def extractNameData(entity: WikidataEntity): (String, String) = {
        (entity.id, entity.label.get)
    }

    /**
      * Transforms triple into a joinable format
      * @param element triple to transform
      * @return joinable tuple
      */
    def makeJoinable(element: (String, String, String)): (String, (String, String)) = {
        (element._3, (element._1, element._2))
    }

    /**
      * Joins entities with id values with the resolvable names and resolves the names if possible.
      * @param idData RDD of entities to resolve
      * @param resolvableNames RDD of resolvable names
      * @return RDD of triples containing resolved names
      */
    def joinIdRDD(
        idData: RDD[(String, (String, String))],
        resolvableNames: RDD[(String, String)]
    ): RDD[(String, String, String)] = {
        idData
            .leftOuterJoin(resolvableNames)
            .map {
                case (valueId, ((entityId, property), labelOption)) =>
                    (entityId, property, labelOption.getOrElse(valueId))
            }
    }

    /**
      * Joins entities with unit values containing ids with the resolvable names and resolves the
      * names if possible.
      * @param unitData RDD of entities to resolve
      * @param nameData RDD of resolvable names
      * @return RDD of triples containing resolved names
      */
    def joinUnitRDD(
        unitData: RDD[(String, (String, String, String))],
        nameData: RDD[(String, String)]
    ): RDD[(String, String, String)] = {
        unitData
            .leftOuterJoin(nameData)
            .map {
                case (unitValueId, ((entityId, property, value), labelOption)) =>
                    (entityId, property, value + ";" + labelOption.getOrElse(unitValueId))
            }
    }

    /**
      * Rebuilds data structure of property field of Wikidata entities
      * @param resolvedEntries RDD of entries with resolved ids
      * @return RDD of tuples containing id and properties
      */
    def rebuildProperties(
        resolvedEntries: RDD[(String, String, String)]
    ): RDD[(String, Map[String, List[String]])] = {
        resolvedEntries
            .map { case (id, property, value) => (id, List((property, value))) }
            .reduceByKey(_ ++ _)
            .map { case (id, propertyList) =>
                val propertyMap = propertyList
                    .groupBy(_._1)
                    .mapValues(_.map(_._2))
                    .map(identity)
                (id, propertyMap)
            }
    }
}
