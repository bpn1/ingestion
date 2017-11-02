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
import org.apache.spark.rdd._
import scala.language.postfixOps
import de.hpi.ingestion.dataimport.wikidata.models.{SubclassEntry, WikidataEntity}
import de.hpi.ingestion.framework.SparkJob

/**
  * Builds the subclass hierarchy of selected Wikidata classes and tags every `WikidataEntity` that is an instance of
  * one of the subclasses with the top level class.
  */
class TagEntities extends SparkJob {
    import TagEntities._
    appName = "TagEntities"
    configFile = "wikidata_import.xml"

    var wikidataEntities: RDD[WikidataEntity] = _
    var subclassEntries: RDD[(String, String, Map[String, List[String]])] = _

    // $COVERAGE-OFF$
    /**
      * Reads the Wikidata entities from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        wikidataEntities = sc.cassandraTable[WikidataEntity](settings("keyspace"), settings("wikidataTable"))
    }

    /**
      * Saves the tagged Wikidata entities to the Cassandra.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        subclassEntries.saveToCassandra(
            settings("keyspace"),
            settings("wikidataTable"),
            SomeColumns("id", "instancetype", "data" append))
    }
    // $COVERAGE-ON$

    /**
      * Tags the WikidataEntities with their superclass.
      * @param sc Spark Context used to e.g. broadcast variables
      * @return List of RDDs containing the output data
      */
    override def run(sc: SparkContext): Unit = {
        val classData = wikidataEntities.map(translateToSubclassEntry)
        val subclassData = classData
            .filter(_.data.contains(subclassProperty))
            .cache
        val subclasses = buildSubclassMap(subclassData, tagClasses)
        subclassEntries = updateEntities(classData, subclasses, settings("wikidataPathProperty"))
    }
}

object TagEntities {
    val instanceProperty = "instance of"
    val subclassProperty = "subclass of"
    val tagClasses = Map(
        "Q268592" -> "economic branch",
        "Q362482" -> "operation",
        "Q179076" -> "exchange",
        "Q650241" -> "financial institutions",
        "Q1664720" -> "institute",
        "Q563787" -> "health maintenance organization",
        "Q79913" -> "non-governmental organization",
        "Q783794" -> "company",
        "Q7275" -> "state",
        "Q1785733" -> "environmental organization",
        "Q15911314" -> "association",
        "Q6881511" -> "enterprise",
        "Q4287745" -> "medical organization",
        "Q2222986" -> "lobbying organization",
        "Q672386" -> "collection agency",
        "Q1331793" -> "media company",
        "Q1123526" -> "chamber of commerce",
        "Q1047437" -> "copyright collective",
        "Q4830453" -> "business enterprise",
        "Q1418640" -> "local government in Germany",
        "Q699386" -> "statutory corporation",
        "Q262166" -> "municipality of Germany"
    )

    /**
      * Adds entries of new class path map to the old map if they are new or shorter than the
      * current path.
      * @param subclasses Map of new subclass path entries
      * @param oldClasses Map of current subclass path entries
      * @return Map of merged subclass path entries containing all new entries and shorter paths
      */
    def addShorterPaths(
        subclasses: Map[String, List[String]],
        oldClasses: Map[String, List[String]]
    ): Map[String, List[String]] = {
        subclasses ++ oldClasses.filterNot { case (key, path) =>
            subclasses.get(key).exists(_.size <= path.size)
        }
    }

    /**
      * Merges two subclass Maps by choosing the shortest Path to the target classes.
      * @param left first subclass Map
      * @param right second subclass Map
      * @return merged subclass Map
      */
    def mergeSubclassMaps(
        left: Map[String, List[String]],
        right: Map[String, List[String]]
    ): Map[String, List[String]] = {
        (left.keySet ++ right.keySet).flatMap { classId =>
            val leftPath = left.get(classId)
            val rightPath = right.get(classId)
            val shorterPath = leftPath
                .filter(l => !rightPath.exists(_.length < l.length))
                .orElse(rightPath)
            shorterPath.map((classId, _))
        }.toMap
    }

    /**
      * Builds a map of subclass paths given a list of subclass entries containing
      * the subclassProperty property.
      * @param categoryData RDD of Subclass Entries containing the subclass information.
      * @param searchClasses Map of superclasses with id and name for which the subclass
      * information is gathered.
      * @return Map of subclass id pointing to the path of superclasses
      */
    def buildSubclassMap(
        categoryData: RDD[SubclassEntry],
        searchClasses: Map[String, String]
    ): Map[String, List[String]] = {
        searchClasses.map { case (wikiDataId, tag) =>
            var oldClasses = Map(wikiDataId -> List(tag))
            var subclasses = Map[String, List[String]]()

            var addedElements = true
            while(addedElements) {
                subclasses ++= addShorterPaths(subclasses, oldClasses)

                // iteratively append next layer of subclass tree
                val newClasses = categoryData
                    .map(entry => entry.copy(classList = entry.data(subclassProperty).filter(oldClasses.contains)))
                    .flatMap { case SubclassEntry(id, label, data, classList) =>
                        classList.headOption
                            .flatMap(oldClasses.get)
                            .map(newClassList => (id, newClassList :+ label))
                    }.collect
                    .toMap

                // check if new elements were added and not stuck in a loop
                addedElements = newClasses.nonEmpty && newClasses.keySet != oldClasses.keySet
                oldClasses = newClasses
            }
            subclasses
        }.reduce(mergeSubclassMaps)
    }

    /**
      * Translates a Wikidata entity into a Subclass entry. This entry contains the id, the label
      * or the id if there is no label and the data entries with either the instanceProperty or
      * subclassProperty key.
      * @param entity Wikidata entity to translate
      * @return SubclassEntry with only the id, label and subclass or instance property
      */
    def translateToSubclassEntry(entity: WikidataEntity): SubclassEntry = {
        val data = entity.data.filterKeys(key =>
            key == instanceProperty || key == subclassProperty)
        val label = entity.label.getOrElse(entity.id)
        SubclassEntry(entity.id, label, data)
    }

    /**
      * Creates value of the instancetype column and creates new data entry containing the subclass
      * path for a Wikidata entity.
      * @param entry Subclass entry containing the id and data of the Wikidata entry
      * @param classes map of class data used to set subclass path and set instancetype
      * @param pathKey value used as property key for the suclass path
      * @return triple of wikidata id, instancetype and path property
      */
    def updateInstanceOfProperty(
        entry: SubclassEntry,
        classes: Map[String, List[String]],
        pathKey: String
    ): (String, String, Map[String, List[String]]) = {
        val classKey = entry.data(instanceProperty)
            .filter(classes.contains)
            .head
        val path = classes(classKey)
        (entry.id, path.head, Map(pathKey -> path))
    }

    /**
      * Updates the instancetype and wikidata path for all entries which are an instance of a class listed in the
      * subclasses Map.
      * @param entries RDD of Subclass Entries to update
      * @param subclasses Map containing the subclasses and their path
      * @param pathKey value used as property key for the suclass path
      * @return RDD of triples containing wikidata id, instancetype and the path property
      */
    def updateEntities(
        entries: RDD[SubclassEntry],
        subclasses: Map[String, List[String]],
        pathKey: String
    ): RDD[(String, String, Map[String, List[String]])] = {
        entries
            .filter(_.data.get(instanceProperty).exists(_.exists(subclasses.contains)))
            .map(updateInstanceOfProperty(_, subclasses, pathKey))
    }
}
