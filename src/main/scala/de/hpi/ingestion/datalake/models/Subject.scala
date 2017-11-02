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

package de.hpi.ingestion.datalake.models

import java.util.UUID
import de.hpi.ingestion.datalake.SubjectManager
import scala.io.Source

case class Subject(
    var id: UUID = UUID.randomUUID(),
    var master: UUID,
    var datasource: String,
    var name: Option[String] = None,
    var aliases: List[String] = Nil,
    var category: Option[String] = None,
    var properties: Map[String, List[String]] = Map(),
    var relations: Map[UUID, Map[String, String]] = Map(),

    var master_history: List[Version] = Nil,
    var name_history: List[Version] = Nil,
    var aliases_history: List[Version] = Nil,
    var category_history: List[Version] = Nil,
    var properties_history: Map[String, List[Version]] = Map(),
    var relations_history: Map[UUID, Map[String, List[Version]]] = Map()
) extends DLImportEntity {
    /**
      * Compares the subject using its uuid
      * @param obj object to compare this subject to
      * @return whether the subject equals the other object
      */
    override def equals(obj: Any): Boolean = obj match {
        case that: Subject => that.id == this.id
        case _ => false
    }

    /**
      * Returns the hash code of the subjects uuid.
      * @return hash code of the id
      */
    override def hashCode(): Int = this.id.hashCode()

    /**
      * Returns the values of an attribute given the name of the attribute.
      * @param attribute name of the attribute to retrieve
      * @return list of the attribute values
      */
    def get(attribute: String): List[String] = {
        if(this.fieldNames[Subject]().contains(attribute)) {
            val field = this.getClass.getDeclaredField(attribute)
            field.setAccessible(true)
            attribute match {
                case "master" => List(field.get(this).asInstanceOf[UUID].toString)
                case "datasource" => List(field.get(this).asInstanceOf[String])
                case "name" | "category" => field.get(this).asInstanceOf[Option[String]].toList
                case "aliases" => field.get(this).asInstanceOf[List[String]]
                case _ => Nil
            }
        } else {
            properties.getOrElse(attribute, Nil)
        }
    }

    /**
      * Returns only the normalized properties of this Subject.
      * @return subset of the properties Map containing only the normalized properties
      */
    def normalizedProperties: Map[String, List[String]] = {
        properties.filterKeys(Subject.normalizedPropertyKeys)
    }

    /**
      * Returns the score of the slave relation to this Subjects master node if it has one.
      * @return score of the master node relation
      */
    def masterScore: Option[Double] = {
        this
            .relations
            .get(this.master)
            .map(_(SubjectManager.slaveKey).toDouble)
    }

    /**
      * Returns the relation relevant for relation aggregation for merging.
      * @return Map containing all relevant relations
      */
    def masterRelations: Map[UUID, Map[String, String]] = {
        this
            .relations
            .mapValues(_.filterKeys(!Subject.relationBlackList(_)))
            .filter(_._2.nonEmpty)
    }

    /**
      * Returns a list of UUIDs of slaves
      * @return List of UUIDs of slaves
      */
    def slaves: List[UUID] = {
        this
            .relations
            .filter(_._2.contains(SubjectManager.masterKey))
            .keys
            .toList
    }

    /**
      * Returns whether the subject is a master node
      * @return Boolean if subject is master
      */
    def isMaster: Boolean = this.id == this.master

    /**
      * Returns whether the subject is a slave node
      * @return Boolean if subject is slave
      */
    def isSlave: Boolean = !isMaster

    /**
      * Transforms the Subjects and its properties into a tab separated string. The tsv format is the following:
      * id, name, aliases, category, property1, property2, ..., propertyN.
      * The sequence of the properties is defined by the file normalized_properties.txt
      * @return Subject as tsv-formatted
      */
    def toTsv: String = {
        val quote = "\""
        var tsvString = s"$quote$id$quote"
        tsvString += s"\t${name.map(value => s"$quote${value.replaceAll("\"", "\\\"")}$quote").getOrElse("")}"
        val tsvAliases = aliases.map(alias => s"$quote${alias.replaceAll("\"", "\\\"")}$quote").mkString(",")
        tsvString += s"\t$tsvAliases"
        tsvString += s"\t${category.map(value => s"$quote${value.replaceAll("\"", "\\\"")}$quote").getOrElse("")}"
        val tsvProperties = Subject.normalizedPropertyKeyList
            .map(get)
            .map { propertyValues =>
                propertyValues
                    .map(propertyValue => s"$quote${propertyValue.replaceAll("\"", "\\\"")}$quote")
                    .mkString(",")
            }.mkString("\t")
        tsvString += s"\t$tsvProperties"
        tsvString
    }
}

/**
  * Companion-Object to the Subject case class containing the keys for all normalized properties.
  */
object Subject {
    val relationBlackList = Set(SubjectManager.masterKey, SubjectManager.slaveKey, SubjectManager.duplicateKey)
    val normalizedPropertyKeyList: List[String] = Source
        .fromURL(this.getClass.getResource("/normalized_properties.txt"))
        .getLines
        .toList
    val normalizedPropertyKeys : Set[String] = normalizedPropertyKeyList.toSet

    /**
      * Creates a default empty Subject with equal id and master
      * @param id uuid for master and id
      * @param datasource datasource of the Subject
      * @return Subject with equal id and master and the given datasource
      */
    def empty(id: UUID = UUID.randomUUID(), datasource: String): Subject = {
        Subject(id = id, master = id, datasource = datasource)
    }

    /**
      * Creates a default master Subject
      * @param id uuid for master and id
      * @return Subject with equal id and master and master as its datasource
      */
    def master(id: UUID = UUID.randomUUID()): Subject = {
        Subject(id = id, master = id, datasource = "master")
    }
}
