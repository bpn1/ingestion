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

package de.hpi.ingestion.datalake

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import de.hpi.ingestion.datalake.models.Subject
import de.hpi.ingestion.framework.SparkJob
import org.apache.spark.rdd.RDD

class CSVExport extends SparkJob {
    import CSVExport._
    appName = "CSVExport v1.0"
    val keyspace = "datalake"
    val tablename = "subject"

    var subjects: RDD[Subject] = _
    var nodes: RDD[String] = _
    var edges: RDD[String] = _

    // $COVERAGE-OFF$
    /**
      * Loads Subjects from the Cassandra.
      * @param sc Spark Context used to load the RDDs
      */
    override def load(sc: SparkContext): Unit = {
        subjects = sc.cassandraTable[Subject](keyspace, tablename)
    }

    /**
      * Saves nodes and edges to the HDFS.
      * @param sc Spark Context used to connect to the Cassandra or the HDFS
      */
    override def save(sc: SparkContext): Unit = {
        nodes.saveAsTextFile("export_nodes")
        edges.saveAsTextFile("export_edges")
    }
    // $COVERAGE-ON$

    /**
      * Creates Nodes and Edges from the Subjects.
      * @param sc Spark Context used to e.g. broadcast variables
      */
    override def run(sc: SparkContext): Unit = {
        val masters = subjects.filter(_.isMaster)
        nodes = masters.map(nodeToCSV)
        edges = masters.flatMap(edgesToCSV)
    }
}

object CSVExport {
    val quote = "\""
    val separator = ","
    val arraySeparator = ";"
    val categoryColors = Map(
        "business" -> "2471A3",
        "organization" -> "A6ACAF",
        "country" -> "27AE60",
        "city" -> "A04000",
        "sector" -> "D68910"
    )
    val relationNormalization = Map(
        "country" -> "located in",
        "located in the administrative territorial entity" -> "located in",
        "headquarters location" -> "headquarters located in",
        "contains administrative territorial entity" -> "contains location",
        "owned by" -> "owned by",
        "stock exchange" -> "stock exchange",
        "parent organization" -> "owns",
        "subsidiary" -> "owns",
        "industry" -> "has industry",
        "country of origin" -> "founded in location",
        "followed by" -> "followed by",
        "follows" -> "follows",
        "parentCompany" -> "owned by",
        "location of formation" -> "founded in location",
        "owningCompany" -> "owned by",
        "owner" -> "owned by",
        "successor" -> "followed by",
        "predecessor" -> "follows",
        "distributingCompany" -> "distributes for",
        "distributingLabel" -> "distributes for",
        "gen_founder" -> "founded by",
        "owner of" -> "owns",
        "manufacturer" -> "manufactured by",
        "division" -> "owned by",
        "business division" -> "owned by",
        "foundedBy" -> "founded by",
        "location" -> "located in",
        "investor" -> "invests in",
        "keyPerson" -> "is key person",
        "central bank" -> "has central bank",
        "parentOrganisation" -> "owned by",
        "airline alliance" -> "owns",
        "locationCity" -> "located in",
        "said to be the same as" -> "same as",
        "foundationPlace" -> "founded in",
        "production company" -> "production company",
        "employer" -> "employed by",
        "founder" -> "founded by",
        "producer" -> "produced by",
        "replaces" -> "follows",
        "replaced by" -> "followed by",
        "childOrganisation" -> "owns"
    )

    /**
      * Parses a Subject into a csv node in the following format:
      * :ID(Subject),name,aliases:string[],category:LABEL,color
      * @param subject Subject to parse
      * @return a comma separated line containing the id, name, aliases and category of the Subject
      */
    def nodeToCSV(subject: Subject): String = {
        val aliasString = subject.aliases
            .mkString(arraySeparator)
            .replace("\\", "")
            .replace("\"", "\\\"")
        val name = subject.name
            .getOrElse("")
            .replace("\"", "'")
            .replaceAll("\\\\", "")
        val category = subject.category
        val color = category.flatMap(categoryColors.get).getOrElse("")
        val output = List(subject.id.toString, name, aliasString, category.getOrElse(""), color)
            .mkString(quote + separator + quote)
        // TODO serialize properties to JSON string
        quote + output + quote
    }

    /**
      * Parses a Subjects relations to multiple csv edges in the following format:
      * :START_ID(Subject),:END_ID(Subject),:TYPE
      * @param subject Subject to parse
      * @return List of comma separated lines of subject id, target id, relation type
      */
    def edgesToCSV(subject: Subject): List[String] = {
        subject
            .masterRelations
            .flatMap { case (id, props) =>
                props.keySet
                    .flatMap(relationNormalization.get)
                    .toList
                    .distinct
                    .map(subject.id + separator + id + separator + _)
            }.map(_.trim)
            .filter(_.nonEmpty)
            .toList
    }
}
