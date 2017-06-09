package de.hpi.ingestion.dataimport.wikidata

import org.apache.spark.SparkContext
import com.datastax.spark.connector._

import scala.util.matching.Regex
import de.hpi.ingestion.dataimport.wikidata.models.WikiDataEntity
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import org.apache.spark.rdd.RDD

/**
  * This job resolves the Wikidata Ids in the properties of each Wikidata entity. Entities labeled
  * with an instancetype in the TagEntities job are not resolved.
  */
object ResolveEntities extends SparkJob {
	appName = "ResolveEntities"
	val keyspace = "wikidumps"
	val tablename = "wikidata"

	// $COVERAGE-OFF$
	/**
	  * Loads the Wikidata entities from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val wikidata = sc.cassandraTable[WikiDataEntity](keyspace, tablename)
		List(wikidata).toAnyRDD()
	}

	/**
	  * Saves the Wikidata entities with resolved ids to the Cassandra.
	  * @param output List of RDDs containing the output of the job
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  * @param args arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[(String, Map[String, List[String]])]()
			.head
			.saveToCassandra(keyspace, tablename, SomeColumns("id", "data"))
	}
	// $COVERAGE-ON$

	/**
	  * Flattens Wikidata entitiy into triples of entity id, property key and property value.
	  * @param entity Wikidata entity to flatten
	  * @return triple of entity id, property key and property value
	  */
	def flattenWikidataEntity(entity: WikiDataEntity): List[(String, String, String)] = {
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
	def shouldBeResolved(entity: WikiDataEntity): Boolean = {
		entity.label.isDefined && entity.instancetype.isEmpty
	}

	/**
	  * Extracts entity id and name of Wikidata entity in a joinable format.
	  * @param entity Wikidata entity to use
	  * @return tuple of wikidata id and entity label
	  */
	def extractNameData(entity: WikiDataEntity): (String, String) = {
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

	/**
	  * Resolves the Wikidata id of untagged entities in the properties of every Wikidata Entity.
	  * @param input List of RDDs containing the input data
	  * @param sc Spark Context used to e.g. broadcast variables
	  * @param args arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val wikidata = input.fromAnyRDD[WikiDataEntity]().head

		val entityData = wikidata.flatMap(flattenWikidataEntity).cache
		val noIdData = entityData.filter(!containsWikidataIdValue(_))
		val noEntityData = noIdData.filter(!hasUnitValue(_))
		val unitData = noIdData
			.filter(hasUnitValue)
			.map(splitUnitValue)
		val resolvableNames = wikidata
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

		val resolvedTuples = rebuildProperties(resolvedData)
		List(resolvedTuples).toAnyRDD()
	}
}
