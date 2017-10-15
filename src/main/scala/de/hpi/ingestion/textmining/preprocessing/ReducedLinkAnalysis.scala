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

package de.hpi.ingestion.textmining.preprocessing

import com.datastax.spark.connector._
import de.hpi.ingestion.framework.{CommandLineConf, SparkJob}
import org.apache.spark.SparkContext

/**
  * Groups the `Links` once on the aliases and once on the pages and saves them to the columns for reduced links.
  */
class ReducedLinkAnalysis extends SparkJob {
	appName = "Reduced Link Analysis"
	configFile = "textmining.xml"

	val linkAnalysis = new LinkAnalysis

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		linkAnalysis.load(sc)
	}

	/**
	  * Saves the grouped aliases and links to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		linkAnalysis.aliases
			.map(alias => (alias.alias, alias.pages))
			.saveToCassandra(settings("keyspace"), settings("linkTable"), SomeColumns("alias", "pagesreduced"))
		linkAnalysis.pages
			.map(page => (page.page, page.aliases))
			.saveToCassandra(settings("keyspace"), settings("pageTable"), SomeColumns("page", "aliasesreduced"))
	}
	// $COVERAGE-ON$

	/**
	  * Groups the links once on the aliases and once on the pages.
	  * @param sc Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		linkAnalysis.conf = CommandLineConf(Seq("-r"))
		linkAnalysis.run(sc)
	}
}
